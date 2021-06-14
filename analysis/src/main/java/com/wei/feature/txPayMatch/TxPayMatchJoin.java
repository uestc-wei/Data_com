package com.wei.feature.txPayMatch;

import com.wei.pojo.OrderEvent;
import com.wei.pojo.ReceiptEvent;
import com.wei.source.DataSourceFactory;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter.Strategy;
import org.apache.flink.util.Collector;
import scala.Tuple2;

public class TxPayMatchJoin {

    public static void main(String[] args) throws Exception {
        ParameterTool startUpParameterTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = DataSourceFactory.getEnv();
        Map<String, DataStream<String>> multiKafkaStream = DataSourceFactory
                .createMultiKafkaStream(startUpParameterTool,
                        SimpleStringSchema.class);
        DataStream<String> orderEventSource  = multiKafkaStream.get("OrderEvent");
        DataStream<String> receiptEventSource = multiKafkaStream.get("ReceiptEvent");

        SingleOutputStreamOperator<OrderEvent> orderEventStream  = orderEventSource.map(line -> {
            String[] fields = line.split(",");
            return new OrderEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
        }).assignTimestampsAndWatermarks(new Strategy<>(
                new BoundedOutOfOrdernessTimestampExtractor<OrderEvent>(Time.of(200, TimeUnit.MILLISECONDS)) {
                    @Override
                    public long extractTimestamp(OrderEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                })).filter(data -> !"".equals(data.getTxId()));

        SingleOutputStreamOperator<ReceiptEvent> receiptEventStream =
                receiptEventSource.map(line -> {
                    String[] fields = line.split(",");
                    return new ReceiptEvent(fields[0], fields[1], new Long(fields[2]));
                })
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<>(
                        new BoundedOutOfOrdernessTimestampExtractor<ReceiptEvent>(Time.of(200, TimeUnit.MILLISECONDS)) {
                            @Override
                            public long extractTimestamp(ReceiptEvent element) {
                                return element.getTimestamp() * 1000L;
                            }
                        }
                ));
        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> result = orderEventStream
                .keyBy(OrderEvent::getTxId)
                .intervalJoin(receiptEventStream.keyBy(ReceiptEvent::getTxId))
                .between(Time.seconds(-3), Time.seconds(5))
                .process(new TxProcessFunction());
        result.print();
        env.execute("tx-join");
    }
    public static class TxProcessFunction extends ProcessJoinFunction<OrderEvent,ReceiptEvent, Tuple2<OrderEvent,ReceiptEvent>> {

        @Override
        public void processElement(OrderEvent left, ReceiptEvent right, Context ctx,
                Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
                out.collect(new Tuple2<>(left,right));
        }
    }
}
