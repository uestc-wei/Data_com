package com.wei.feature.flowCount;

import com.wei.pojo.PageViewCount;
import com.wei.pojo.UserBehavior;
import com.wei.util.DataSourceFactory;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter.Strategy;
import org.apache.flink.util.Collector;

public class FlowCount {

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        DataStreamSource<String> source = new DataSourceFactory(parameterTool).kafkaStringSourceProduce();

        //
       DataStream<UserBehavior> userBehaviorDataStream = source.map(line -> {
            String[] split = line.split(",");
            return new UserBehavior(new Long(split[0]), new Long(split[1]),
                    new Integer(split[2]), split[3], new Long(split[4]));
        }).assignTimestampsAndWatermarks(
                new Strategy<>(new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(Time.of(200,
                        TimeUnit.MILLISECONDS)) {
                    @Override
                    public long extractTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;
                    }
                }));

        //基础分组聚合
        DataStream<Tuple2<String, Long>> pvResultStream = userBehaviorDataStream
                .filter(userBehavior -> "pv".equals(userBehavior.getBehavior()))
                //单key聚合，无法并行
                .map((MapFunction<UserBehavior, Tuple2<String, Long>>) value -> new Tuple2<>("pv",1L))
                .keyBy(item -> item.f0)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .sum(1);

        //随机化窗口聚合key值
        SingleOutputStreamOperator<PageViewCount> aggResult = userBehaviorDataStream
                .filter(userBehavior -> "pv".equals(userBehavior.getBehavior()))
                .map(new MapFunction<UserBehavior, Tuple2<Integer, Long>>() {
                    @Override
                    public Tuple2<Integer, Long> map(UserBehavior value) throws Exception {
                        Random random = new Random();
                        return new Tuple2<>(random.nextInt(10), 1L);
                    }
                })
                //分区keyBy
                .keyBy(data -> data.f0)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                //自定义聚合函数和窗口函数，修改输出格式
                .aggregate(new PvCountAgg(), new PvWindowFunction());
        //分区聚合
        SingleOutputStreamOperator<PageViewCount> totalResult = aggResult.keyBy(PageViewCount::getWindowEnd)
                .process(new TotalPvCount());
        //sink
        totalResult.print();

        source.getExecutionEnvironment().execute("pv count job ");
    }
    public static class PvCountAgg implements AggregateFunction<Tuple2<Integer,Long>,Long,Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<Integer, Long> value, Long accumulator) {
            return accumulator+1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a+b;
        }
    }
    public static class PvWindowFunction implements WindowFunction<Long, PageViewCount,Integer, TimeWindow>{

        @Override
        public void apply(Integer integer, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out) {
                out.collect(new PageViewCount(integer.toString(),window.getEnd(),input.iterator().next()));
        }
    }
    public static class TotalPvCount extends KeyedProcessFunction<Long,PageViewCount,PageViewCount>{
        //定义valueState保存当前count值
        ValueState<Long> totalState;

        @Override
        public void open(Configuration parameters) throws Exception {
            totalState=getRuntimeContext().getState(new ValueStateDescriptor<>("total-count", Long.class));
        }

        @Override
        public void processElement(PageViewCount value, Context ctx, Collector<PageViewCount> out) throws Exception {
            Long totalCount = totalState.value();
            if (totalCount==null){
                totalCount=0L;
                totalState.update(totalCount);
            }
            totalState.update(totalCount+value.getCount());
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd()+1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<PageViewCount> out) throws Exception {
            Long totalCount = totalState.value();
            out.collect(new PageViewCount("pv",ctx.getCurrentKey(),totalCount));
            //每个时间窗口之后清楚缓存
            totalState.clear();
        }
    }
}
