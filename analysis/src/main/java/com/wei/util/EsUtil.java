package com.wei.util;


import com.wei.pojo.OrderEvent;
import com.wei.pojo.ReceiptEvent;
import com.wei.sink.Entity.EsSinkTransaction;
import com.wei.util.EsUtil.EsHostInfo.EsHostInfoUserBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class EsUtil {

    private static final Logger logger = LoggerFactory.getLogger(EsUtil.class);

    private static final Set<String> indexNameCache = new HashSet<>();

    private static RestHighLevelClient esClient;

    //建造者
    public static class EsHostInfo {
        private  List<HttpHost> httpInfoList = new ArrayList<>();

        private EsHostInfo(List<HttpHost> httpInfoList){
            this.httpInfoList=httpInfoList;
        }

        public static EsHostInfoUserBuilder newBuilder(){
            return new EsHostInfoUserBuilder();
        }

        public static class EsHostInfoUserBuilder{
            private final List<HttpHost> httpInfoList = new ArrayList<>();
            private EsHostInfoUserBuilder(){
            }

            public EsHostInfoUserBuilder addEsHttpInfo(String host,int port){
                httpInfoList.add(new HttpHost(host,port));
                return this;
            }

            //可以做一些过滤操作
            public HttpHost[] build(){
                //return new EsHostInfo(httpInfoList);
                return httpInfoList.toArray(new HttpHost[0]);
            }
        }
    }


    public static RestHighLevelClient createRestHighLevelClient(HttpHost[] hosts){
        /*// 设置验证信息，填写账号及密码
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        String userName = ConfigUtil.getEsUserName();
        String password = ConfigUtil.getEsPassword();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(userName, password));
        logger.info("create es client: username={} passwd={}", userName, password);
       */
        RestClientBuilder builder = RestClient.builder(hosts);
        /*// 设置认证信息
        builder.setHttpClientConfigCallback(
                httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));*/
        return new RestHighLevelClient(builder);
    }

    public static RestHighLevelClient buildEsClient(){
        String esHost = ConfigUtil.getEsHost();
        EsHostInfoUserBuilder esHostInfoUserBuilder = EsHostInfo.newBuilder();
        Arrays.stream(esHost.split(",")).forEach(
                s->{
                    String[] esPair = s.split(":");
                    String host = esPair[0];
                    int port = Integer.parseInt(esPair[1]);
                    esHostInfoUserBuilder.addEsHttpInfo(host,port);
                }
        );
        return createRestHighLevelClient(esHostInfoUserBuilder.build());
    }

    /**
     * 功能描述: 提交批处理请求到ES
     *
     * @param: RestHighLevelClient client, BulkRequest request
     * @return: BulkResponse
     * @auther: ziggywu
     * @date:
     */
    public static BulkResponse commitBulkRequestToES(RestHighLevelClient client,
            BulkRequest request) throws IOException {
        return client.bulk(request, RequestOptions.DEFAULT);
    }

    /**
     * 构建es content
     * @param orderEventReceiptEventTuple
     * @return
     * @throws IOException
     */
    private static XContentBuilder builderXContent(Tuple2<OrderEvent, ReceiptEvent> orderEventReceiptEventTuple)
            throws IOException {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        OrderEvent orderEvent = orderEventReceiptEventTuple._1;
        ReceiptEvent receiptEvent = orderEventReceiptEventTuple._2;
        xContentBuilder.startObject();
        xContentBuilder.field("txId",orderEvent.getTxId());
        xContentBuilder.field("orderId",orderEvent.getOrderId());
        xContentBuilder.field("eventType",orderEvent.getEventType());
        xContentBuilder.field("orderTime",orderEvent.getTimestamp());
        xContentBuilder.field("payChannel",receiptEvent.getPayChannel());
        xContentBuilder.field("receiptTime",receiptEvent.getTimestamp());
        xContentBuilder.endObject();
        return xContentBuilder;
    }
    /**
     *
     * @param esClient
     * @param transaction
     * @return
     */
    public static BulkRequest[] createBulkRequestBatch(RestHighLevelClient esClient,
            EsSinkTransaction transaction) throws IOException {
        int esBulkRequestMaxSize = ConfigUtil.getEsBulkRequestMaxSize();
        int bufferSize = transaction.getCommitBuffer().size();
        int bulkBatchSize = (int) Math.ceil((float) bufferSize / esBulkRequestMaxSize);
        BulkRequest[] bulkRequestBatch = new BulkRequest[bulkBatchSize];
        Arrays.setAll(bulkRequestBatch, (i) -> new BulkRequest());

        Map<String, Tuple2<OrderEvent, ReceiptEvent>> dataWaitToCommit = transaction.getCommitBuffer();
        int i = 0 ;
        for (Map.Entry<String, Tuple2<OrderEvent, ReceiptEvent>> entry : dataWaitToCommit.entrySet()) {
            int batchIndex= i / esBulkRequestMaxSize;
            String esDeduplicateId = entry.getKey();
            Tuple2<OrderEvent, ReceiptEvent> orderEventReceiptEventTuple = entry.getValue();
            String indexName = ConfigUtil.getEsIndexName() + "_"
                    + DateUtils.getDateForDay(orderEventReceiptEventTuple._1.getTimestamp() * 1000);
            String indexType = ConfigUtil.getEsIndexTypeName();
            //判断索引是否存在，如果cache中不存在则去查询es中是否存在，es中不存在则会创建索引
            createEsIndexIfAbsent(esClient,indexName,indexType);

            IndexRequest indexRequest = new IndexRequest(indexName, indexType, esDeduplicateId);
            XContentBuilder xContentBuilder = builderXContent(orderEventReceiptEventTuple);
            indexRequest.source(xContentBuilder);

            bulkRequestBatch[batchIndex].add(indexRequest);
            bulkRequestBatch[batchIndex].waitForActiveShards(ActiveShardCount.ALL);
            i++;
        }
        return bulkRequestBatch;
    }

    /**
     * 创建es索引
     */
    private static void createEsIndexIfAbsent(RestHighLevelClient esClient, String indexName,
                                     String indexType) throws IOException {
        if (indexNameCache.contains(indexName)){
            return;
        }
        GetIndexRequest getRequest = new GetIndexRequest();
        getRequest.indices(indexName);
        getRequest.types(indexType);

        if (esClient.indices().exists(getRequest,RequestOptions.DEFAULT)){
            indexNameCache.add(indexName);
        }else {
            CreateIndexRequest indexRequest = new CreateIndexRequest(indexName);
            indexRequest.settings(Settings.builder().put("index.number_of_shards",ConfigUtil.getEsNumOfShards()));

        }
    }

    /**
     * 这里可以动态生成索引mapping
     * 创建es 索引mapping
     */
    private static XContentBuilder buildIndexMapping(String indexType) throws IOException {
        XContentBuilder mapping = JsonXContent.contentBuilder()
                .startObject()
                .startObject(indexType)
                .startObject("properties")
                .startObject("txId").field("type", "keyword").endObject()
                .startObject("orderId").field("type", "long").endObject()
                .startObject("eventType").field("type", "keyword").endObject()
                .startObject("orderTime").field("type", "long").endObject()
                .startObject("payChannel").field("type", "keyword").endObject()
                .startObject("receiptTime").field("type", "long").endObject()
                .endObject()
                .endObject()
                .endObject();
        return mapping;
    }
}
