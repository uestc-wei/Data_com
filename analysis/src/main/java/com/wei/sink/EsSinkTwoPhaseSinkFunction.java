package com.wei.sink;

import com.wei.Exception.CommitException;
import com.wei.Exception.CommonException;
import com.wei.Exception.ErrorCodeEnum;

import com.wei.pojo.OrderEvent;
import com.wei.pojo.ReceiptEvent;
import com.wei.sink.Entity.EsSinkContext;
import com.wei.sink.Entity.EsSinkTransaction;
import com.wei.util.ConfigUtil;
import com.wei.util.DateUtils;
import com.wei.util.EsUtil;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.configuration.Configuration;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * Es 二阶段提交
 */
public class EsSinkTwoPhaseSinkFunction extends TwoPhaseEsSinkFunction<Tuple2<OrderEvent, ReceiptEvent>, EsSinkTransaction, EsSinkContext> {

    private final Logger logger = LoggerFactory.getLogger(EsSinkTwoPhaseSinkFunction.class);
    //es
    private transient RestHighLevelClient esClient;
    //异步提交es
    private transient ExecutorService executorService;
    //初始化es算子标识
    private volatile boolean isInit = false ;
    //当前提交时间
    long commitStartTime;
    /**
     * 使用kryo序列化器序列化状态，即事务和上下文环境
     */
    public EsSinkTwoPhaseSinkFunction(){
        super(new KryoSerializer<>(EsSinkTransaction.class,new ExecutionConfig()),
                new KryoSerializer<>(EsSinkContext.class,new ExecutionConfig()));
    }

    /**
     * 初始化：初始化rocksDB线程池,ES client,es线程池
     */
    private synchronized void init() {
        if (isInit){
            return;
        }
        logger.info("init flushDB thread pool");
        flushRocksDBThreadPool= Executors.newFixedThreadPool(1);
        logger.info("init es client");
        esClient= EsUtil.buildEsClient();
        logger.info("init es flush thread pool");
        executorService = Executors.newFixedThreadPool(ConfigUtil.getFlushEsThread());
        isInit=true;
    }


    /**
     * open方法里做一些初始化工作
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        ConfigUtil.setStartUpParameterInRuntime(getRuntimeContext());
        super.open(parameters);
        init();
    }

    /**
     * 处理每一条数据
     * @param transaction
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    protected void invoke(EsSinkTransaction transaction, Tuple2<OrderEvent, ReceiptEvent> value, Context context) throws Exception {
        RuntimeContext runtimeContext = getRuntimeContext();
        int parallelId = runtimeContext.getIndexOfThisSubtask();
        transaction.addDataToCommitBuffer(value,parallelId);
    }

    /**
     * 开启一个新的事务，上一个事务预提交之后和算子初始化时
     * @return
     * @throws Exception
     */
    @Override
    protected EsSinkTransaction beginTransaction() throws Exception {
        EsSinkContext esSinkContext = getUserContext()
                .orElseThrow(() -> new Exception("not set EsSinkContext"));
        EsSinkTransaction transaction = new EsSinkTransaction();
        RuntimeContext runtimeContext = getRuntimeContext();
        transaction.setTransactionId(esSinkContext.genAndIncreaseTransactionId(runtimeContext.getIndexOfThisSubtask()));
        logger.info("start new transaction:{},transactionId:{}",Thread.currentThread().getName(),
                transaction.getTransactionId());
        return transaction;
    }

    //预提交
    @Override
    protected void preCommit(EsSinkTransaction transaction) throws Exception {
        try {
            logger.info("start pre-committing in operator:{},transactionId:{}",
                    getRuntimeContext().getTaskNameWithSubtasks(),transaction.getTransactionId());

            //判断事务提交缓存中是否有数据并且设置状态，提交时会根据状态判断是否有数据提交
            if (transaction.getCommitBuffer().isEmpty()){
                //事务缓存中没有数据
                transaction.setBufferIsEmpty(true);
                logger.info("there is no data in current transaction,"
                        +"operator:{},transactionId:{}",getRuntimeContext().getTaskNameWithSubtasks(),
                        transaction.getTransactionId());
            }else {
                logger.info("buffer size of pre-commit:{}, operator:{},transactionId:{}",
                        transaction.getCommitBuffer().size(),
                        getRuntimeContext().getTaskNameWithSubtasks(),
                        transaction.getTransactionId());
            }
        }catch (Exception e){
            logger.error("Exception occurred in pre-commit stage"
                    + " operator:" + getRuntimeContext().getTaskNameWithSubtasks()
                    + " transactionId:" + transaction.getTransactionId(), e);
            throw new CommonException(ErrorCodeEnum.PRE_COMMIT_ERROR);
        }
    }

    /**
     * 正式提交,将预提交的事务提交至ES
     * @param transaction
     */
    @Override
    protected void commit(EsSinkTransaction transaction) throws ExecutionException, InterruptedException {
        if (!isInit){
            init();
        }
        commitStartTime = System.currentTimeMillis();

        if (transaction.isBufferIsEmpty()){
            logger.info("there is no data in current transaction,"
                            +"operator:{},transactionId:{}",getRuntimeContext().getTaskNameWithSubtasks(),
                    transaction.getTransactionId());
            return;
        }

        //提交数据至ES
        BulkRequest[] bulkRequestBatch = createBulkRequestBatchWithRetry(esClient, transaction,
                getRuntimeContext());
        //提交es
        commitToEs(bulkRequestBatch,esClient,getRuntimeContext(),transaction);
    }

    //
    @Override
    protected void abort(EsSinkTransaction transaction) {
    }


    /**
     * 创建es commit Bulk ,无限重试
     * @param esClient
     * @param transaction
     * @param runtimeContext
     * @return
     */
    private BulkRequest[] createBulkRequestBatchWithRetry(RestHighLevelClient esClient,
            EsSinkTransaction transaction,RuntimeContext runtimeContext){
        BulkRequest[] bulkRequestBatch;
        while (true){
            try {
                //组装bulkRequest
                bulkRequestBatch = EsUtil.createBulkRequestBatch(esClient, transaction);
                logger.info( "successfully set bulk requests in "
                                + "operator:{},transactionId:{},ready to commit",
                        runtimeContext.getTaskNameWithSubtasks(), transaction.getTransactionId());
                break;
            }catch (IOException e) {
                e.printStackTrace();
                logger.error("error occurred in operator:{}, "
                                + "transactionId:{} while creating bulk request batch:{}",
                        runtimeContext.getTaskNameWithSubtasks(),
                        transaction.getTransactionId(), e);
            }
        }
        return bulkRequestBatch;
    }

    private void commitToEs(BulkRequest[] bulkRequestBatch,RestHighLevelClient esClient,
            RuntimeContext runtimeContext,EsSinkTransaction transaction)
            throws ExecutionException, InterruptedException {
        List<BulkResponse> bulkResponses;
        //异步
        if(ConfigUtil.getFlushEsMode() == 1){
            bulkResponses = commitToEsAsynchronously(bulkRequestBatch,esClient,runtimeContext,transaction);
        } else {
            //同步
            bulkResponses = commitToEsSynchronously(bulkRequestBatch,esClient,runtimeContext,transaction);
        }
        //打印response
    }

    /**
     * 异步提交到es
     * @param bulkRequestBatch
     * @param esClient
     * @param runtimeContext
     * @param transaction
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private List<BulkResponse> commitToEsAsynchronously(BulkRequest[] bulkRequestBatch,
            RestHighLevelClient esClient, RuntimeContext runtimeContext, EsSinkTransaction transaction)
            throws ExecutionException, InterruptedException {

        List<CompletableFuture<BulkResponse>> commitFutures = Arrays.stream(bulkRequestBatch)
                .map(bulks -> CompletableFuture.completedFuture(bulks).thenApplyAsync(
                        request -> {
                            BulkResponse commitResponse;
                            //出现异常无限重试
                            while (true) {
                                try {
                                    commitResponse = EsUtil
                                            .commitBulkRequestToES(esClient, request);
                                    if (commitResponse.hasFailures()){
                                        logger.info("bulk commit es failure,recommitting...");
                                        Thread.sleep(1000);
                                        continue;
                                    }
                                    break;
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    logger.error("bulk commit es error",e);
                                }
                            }
                            return commitResponse;
                        }, executorService)).collect(Collectors.toList());
        CompletableFuture<Void> allBulksFuture = CompletableFuture
                .allOf(commitFutures.toArray(new CompletableFuture[commitFutures.size()]))
                .whenComplete((r, e) -> {
                    if (e != null) {
                        throw new CommitException(e);
                    } else {
                        logger.info(
                                "Procedure of committing bulk batch to ES in operator:{} "
                                        + "has completed, transaction id:{} ",
                                runtimeContext.getTaskNameWithSubtasks(),
                                transaction.getTransactionId());
                    }
                });

        //获取提交结果
        CompletableFuture<List<BulkResponse>> listCompletableFuture = allBulksFuture
                .thenApply(future -> commitFutures.stream().map(CompletableFuture::join)
                        .collect(Collectors.toList()));
        return listCompletableFuture.get();
    }

    /**
     * 功能描述: 同步提交bulk批次到ES
     *
     * @param: BulkRequest[] bulkRequestBatch, RestHighLevelClient esClient,RuntimeContext
     *         runtimeContext, EsSinkTransaction transaction
     * @return: void
     * @auther: ziggywu
     * @date:
     */
    private List<BulkResponse> commitToEsSynchronously(BulkRequest[] bulkRequestBatch,
            RestHighLevelClient esClient,
            RuntimeContext runtimeContext, EsSinkTransaction transaction) {
        List<BulkResponse> bulkResponseList = new ArrayList<>();
        //每个批次按顺序提交
        for (BulkRequest bulkRequest : bulkRequestBatch) {
            while (true) {
                try {
                    //调用es客户端，提交bulk到es
                    BulkResponse commitResponse = EsUtil
                            .commitBulkRequestToES(esClient, bulkRequest);

                    //提交失败则重新提交，无限重试
                    if (commitResponse.hasFailures()) {
                        Thread.sleep(1000);
                        continue;
                    }
                    bulkResponseList.add(commitResponse);
                    break;
                } catch (Exception e) {

                    if (judgeTimeout()) {
                        logger.error("commit retry timeout, job will fail");
                        throw new CommitException("commit es retry timeout");
                    }

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException interruptedException) {
                        interruptedException.printStackTrace();
                    }
                }
            }
        }
        return bulkResponseList;
    }



    private boolean judgeTimeout() {
        return System.currentTimeMillis() - commitStartTime
                > ConfigUtil.getCheckpointRetryTimeout() * 1000 * 60;
    }


}
