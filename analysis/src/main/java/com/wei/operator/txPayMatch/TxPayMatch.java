package com.wei.operator.txPayMatch;

import com.wei.pojo.OrderEvent;
import com.wei.pojo.ReceiptEvent;
import com.wei.source.DataSourceFactory;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter.Strategy;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scala.Tuple2;

public class TxPayMatch {
    //定义侧输出流标签
    private final static OutputTag<OrderEvent> unmatchedPays=new OutputTag<>("unmatched-pays");
    private final static OutputTag<ReceiptEvent> unmatchedReceipts=new OutputTag<>("unmatched-receipt");
    public static void main(String[] args) throws Exception{
        //转换pojo
        //分别读取两条输出流
        ParameterTool startUpParameterTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = DataSourceFactory.getEnv();
        Map<String, DataStream<String>> multiKafkaStream = DataSourceFactory
                .createMultiKafkaStream(startUpParameterTool,
                        SimpleStringSchema.class);
        DataStream<String> orderEventSource  = multiKafkaStream.get("OrderEvent");
        DataStream<String> receiptEventSource = multiKafkaStream.get("ReceiptEvent");

        //抽取对象
        //第一条流
        SingleOutputStreamOperator<OrderEvent> orderEventStream = orderEventSource.map(line -> {
            String[] split = line.split(",");
            return new OrderEvent(new Long(split[0]), split[1], split[2], new Long(split[3]));
        }).assignTimestampsAndWatermarks(new Strategy<>(new BoundedOutOfOrdernessTimestampExtractor<OrderEvent>(
                Time.of(300, TimeUnit.MILLISECONDS)) {
            @Override
            public long extractTimestamp(OrderEvent element) {
                return element.getTimestamp() * 1000L;
            }
        })).filter(data-> !"".equals(data.getTxId()));
        //第二条流
        SingleOutputStreamOperator<ReceiptEvent> receiptEventStream = receiptEventSource
                .map(line -> {
                    String[] split = line.split(",");
                    return new ReceiptEvent(split[0], split[1], new Long(split[2]));
                }).assignTimestampsAndWatermarks(new Strategy<>(
                        new BoundedOutOfOrdernessTimestampExtractor<ReceiptEvent>(Time.of(300, TimeUnit.MILLISECONDS)) {
                            @Override
                            public long extractTimestamp(ReceiptEvent element) {
                                return element.getTimestamp() * 1000L;
                            }
                        }));
        //双流connect
        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> resultStream = orderEventStream
                .keyBy(OrderEvent::getTxId)
                .connect(receiptEventStream.keyBy(ReceiptEvent::getTxId))
                .process(new txPayMatchFunction());

        resultStream.print("matched-pays");
        resultStream.getSideOutput(unmatchedPays).print("unmatched-pays");
        receiptEventStream.getSideOutput(unmatchedReceipts).print("unmatched-receipts");

    }

    public static class txPayMatchFunction extends CoProcessFunction<OrderEvent,ReceiptEvent,Tuple2<OrderEvent,ReceiptEvent>> {

        ValueState<OrderEvent> payState;
        ValueState<ReceiptEvent> receiptState;
        @Override
        public void open(Configuration parameters) throws Exception {
            payState=getRuntimeContext().getState(new ValueStateDescriptor<>("pay", OrderEvent.class));
            receiptState=getRuntimeContext().getState(new ValueStateDescriptor<>("receipt", ReceiptEvent.class));
        }

        @Override
        public void processElement1(OrderEvent value, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out)
                throws Exception {
            //订单事件来临后，判断是否有对应的到账信息
            ReceiptEvent receiptEvent = receiptState.value();
            if (receiptEvent!=null){
                //receiptState不为空，说明已经匹配到对应的到账事件，将对应的时间输出，清空当前的状态
                out.collect(new Tuple2<>(value,receiptEvent));
                payState.clear();
                receiptState.clear();
            }else {
                //如果receipt没来，注册定时器等待对应的事件
                ctx.timerService().registerEventTimeTimer((value.getTimestamp()+5)*1000L);//等待时间5s
                //更新状态
                payState.update(value);
            }
        }

        @Override
        public void processElement2(ReceiptEvent value, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out)
                throws Exception {
            //到账事件到来后
            OrderEvent pay = payState.value();
            if (pay!=null){
                out.collect(new Tuple2<>(pay,value));
                payState.clear();
                receiptState.clear();
            }else {
                //如果pay没来，则注册一个定时器
                ctx.timerService().registerEventTimeTimer((value.getTimestamp()+3)*1000L);
                receiptState.update(value);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out)
                throws Exception {
            //定时器触发，说明有其中一个事件没有到来或者之前已经来过了，将未匹配事件输出至侧输出流
            if (payState.value()!=null){
                ctx.output(unmatchedPays,payState.value());
            }
            if (receiptState.value()!=null){
                ctx.output(unmatchedReceipts,receiptState.value());
            }
            payState.clear();
            receiptState.clear();
        }
    }

}
