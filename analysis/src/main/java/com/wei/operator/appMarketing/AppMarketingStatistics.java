package com.wei.operator.appMarketing;

import com.wei.pojo.ChannelPromotionCount;
import com.wei.pojo.MarketingUserBehavior;
import com.wei.source.DataSourceFactory;
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter.Strategy;
import org.apache.flink.util.Collector;

public class AppMarketingStatistics {

    public static void main(String[] args) throws Exception {
        ParameterTool startUpParameterTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = DataSourceFactory.getEnv();
        DataStream<String> source = DataSourceFactory.createKafkaStream(startUpParameterTool,
                SimpleStringSchema.class);

        SingleOutputStreamOperator<MarketingUserBehavior> userBehaviorStream = source.map(line -> {
            String[] split = line.split(",");
            return new MarketingUserBehavior(new Long(split[0]),split[1],split[2],new Long(split[3]));
        }).assignTimestampsAndWatermarks(new Strategy<>(
                new BoundedOutOfOrdernessTimestampExtractor<MarketingUserBehavior>(Time.of(200, TimeUnit.MILLISECONDS)) {
                    @Override
                    public long extractTimestamp(MarketingUserBehavior element) {
                        return element.getTimestamp() * 1000L;
                    }
                }
        ));

        //开窗统计总量
        SingleOutputStreamOperator<ChannelPromotionCount> result = userBehaviorStream
                .filter(data -> !"UNINSTALL".equals(data.getBehavior()))
                .map(new MapFunction<MarketingUserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(MarketingUserBehavior value) throws Exception {
                        return new Tuple2<>("total", 1L);
                    }
                }).keyBy(tuple -> tuple.f0)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new MarketingStatisticsAgg(), new MarketingStatisticsResult());
        result.print();
        env.execute("Channel Promotion Count Job");
    }

    public static class MarketingStatisticsAgg implements AggregateFunction<Tuple2<String,Long>,Long,Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<String, Long> value, Long accumulator) {
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

    public static class MarketingStatisticsResult implements WindowFunction<Long, ChannelPromotionCount,String, TimeWindow>{

        @Override
        public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<ChannelPromotionCount> out)
                throws Exception {
            String windowEnd = new Timestamp(window.getEnd()).toString();
            Long count = input.iterator().next();
            out.collect(new ChannelPromotionCount("total","total",windowEnd,count));
        }
    }
}
