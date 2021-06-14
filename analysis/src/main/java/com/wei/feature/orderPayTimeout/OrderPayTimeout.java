package com.wei.feature.orderPayTimeout;

import com.wei.pojo.OrderEvent;
import com.wei.pojo.OrderResult;
import com.wei.source.DataSourceFactory;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class OrderPayTimeout {

    private final static OutputTag<OrderResult> orderTimeoutTag= new OutputTag<OrderResult>("order-timeout");
    public static void main(String[] args) throws Exception {
        ParameterTool startUpParameterTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = DataSourceFactory.getEnv();
        DataStream<String> source = DataSourceFactory.createKafkaStream(startUpParameterTool,
                SimpleStringSchema.class);
        DataStream<OrderEvent> orderEventDataStream = source
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<>(
                        new BoundedOutOfOrdernessTimestampExtractor<OrderEvent>(Time.of(200, TimeUnit.MILLISECONDS)) {
                            @Override
                            public long extractTimestamp(OrderEvent element) {
                                return element.getTimestamp() * 1000L;
                            }
                        }
                ));
        //processFunction
        SingleOutputStreamOperator<OrderResult> resultStream = orderEventDataStream.keyBy(OrderEvent::getOrderId)
                .process(new OrderPayMatchDetect());

        resultStream.print("pay normally");
        resultStream.getSideOutput(orderTimeoutTag).print("timeout");

        env.execute("order timeout detect without cep job");
    }
    public static class OrderPayMatchDetect extends KeyedProcessFunction<Long,OrderEvent,OrderResult>{

        ValueState<Boolean> isPayedState;
        ValueState<Boolean> isCreatedState;

        //定义保存时间戳状态
        ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            isPayedState = getRuntimeContext()
                    .getState(new ValueStateDescriptor<Boolean>("is-payed", Boolean.class, false));
            isCreatedState=getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-created",Boolean.class,false));
            timerTsState=getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts", Long.class));
        }

        @Override
        public void processElement(OrderEvent value, Context ctx, Collector<OrderResult> out) throws Exception {
           //先获取当前状态
            Boolean isPayed = isPayedState.value();
            Boolean isCreated = isCreatedState.value();
            Long timerTs = timerTsState.value();

            //判断当前事件类型
            if ("create".equals(value.getEventType())){
                //1.如果created,判断是否支付
                if (isPayed){
                    //1.1 如果已经正常支付过，则是正常匹配结果
                    out.collect(new OrderResult(value.getOrderId(),"payed successfully"));
                    isCreatedState.clear();
                    isPayedState.clear();
                    timerTsState.clear();
                }else {
                    //1.2如果没有支付，则注册等待15分钟的定时器，开始等待支付
                    Long ts=(value.getTimestamp()+15*60)*1000L;
                    ctx.timerService().registerEventTimeTimer(ts);
                    //更新状态
                    timerTsState.update(ts);
                    isCreatedState.update(true);
                }
            }else if ("pay".equals(value.getEventType())){
                //2.如果是pay事件，判断是否之前有下单事件
                if (isCreated){
                    //2.1已经有下单事件，要继续判断是否超过15分钟
                    if (value.getTimestamp()*1000L<timerTs){
                        out.collect(new OrderResult(value.getOrderId(),"payed successfully"));
                    }else {
                        ctx.output(orderTimeoutTag,new OrderResult(value.getOrderId(),"payed but already timeout"));
                    }
                    isCreatedState.clear();
                    isPayedState.clear();
                    timerTsState.clear();
                    ctx.timerService().deleteEventTimeTimer(timerTs);
                }else {
                    //2.2没有下单事件，注册定时器等待下单事件
                    ctx.timerService().registerEventTimeTimer(value.getTimestamp()*1000L);
                    //更新状态
                    timerTsState.update(value.getTimestamp()*1000L);
                    isPayedState.update(true);
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<OrderResult> out) throws Exception {
            //定时器触发，说明有一个事件没来
            if (isPayedState.value()){
                //pay来了，create没来
                ctx.output(orderTimeoutTag,new OrderResult(ctx.getCurrentKey(),"payed but not found created log"));
            }else {
                //如果pay没来，支付超时
                ctx.output(orderTimeoutTag,new OrderResult(ctx.getCurrentKey(),"timeout"));
            }
            isCreatedState.clear();
            isPayedState.clear();
            timerTsState.clear();
        }
    }
}

