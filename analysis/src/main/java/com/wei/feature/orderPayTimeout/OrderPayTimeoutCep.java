package com.wei.feature.orderPayTimeout;

import com.wei.pojo.OrderEvent;
import com.wei.pojo.OrderResult;
import com.wei.util.DataSourceFactory;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.OutputTag;

public class OrderPayTimeoutCep {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        DataSourceFactory.init(parameterTool);
        DataStreamSource<String> source = DataSourceFactory.kafkaStringSource(env);
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


        //定义时间限制的模式
        Pattern<OrderEvent, OrderEvent> orderPayPattern = Pattern.<OrderEvent>begin("create")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "create".equals(value.getEventType());
                    }
                }).followedBy("pay").where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "pay".equals(value.getEventType());
                    }
                }).within(Time.minutes(5));

        //定义侧输出流标签，用来表示超时事件
        OutputTag<OrderResult> orderTimeOutTag=new OutputTag<OrderResult>("order-timeout");

        //将pattern应用到输入流中，得到pattern stream
        PatternStream<OrderEvent> patternStream = CEP
                .pattern(orderEventDataStream.keyBy(OrderEvent::getOrderId), orderPayPattern);

        //调用select方法,实现对匹配复杂事件和超时复杂事件的提取和处理
        SingleOutputStreamOperator<OrderResult> resultStream = patternStream
                .select(orderTimeOutTag, new OrderTimeoutSelect(), new OrderPaySelect());

        resultStream.print("payed normally");
        resultStream.getSideOutput(orderTimeOutTag).print("timeOut");

        env.execute("order timeout detect job");
    }

    public static class OrderTimeoutSelect implements PatternTimeoutFunction<OrderEvent,OrderResult>{

        @Override
        public OrderResult timeout(Map<String, List<OrderEvent>> pattern, long timeOutTimeStamp) throws Exception {
            Long timeoutOrderId = pattern.get("create").iterator().next().getOrderId();
            return new OrderResult(timeoutOrderId,"timeout"+timeOutTimeStamp);
        }
    }

    public static class OrderPaySelect implements PatternSelectFunction<OrderEvent,OrderResult>{

        @Override
        public OrderResult select(Map<String, List<OrderEvent>> pattern) throws Exception {
            Long payedOrderId = pattern.get("pay").iterator().next().getOrderId();
            return new OrderResult(payedOrderId,"payId");
        }
    }

}
