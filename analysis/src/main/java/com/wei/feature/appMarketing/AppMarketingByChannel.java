package com.wei.feature.appMarketing;

import com.wei.pojo.ChannelPromotionCount;
import com.wei.pojo.MarketingUserBehavior;
import com.wei.source.DataSourceFactory;
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter.Strategy;
import org.apache.flink.util.Collector;

public class AppMarketingByChannel {

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


        //分渠道开窗统计
        SingleOutputStreamOperator<ChannelPromotionCount> aggregateStream = userBehaviorStream
                .filter(data -> !"UNINSTALL".equals(data.getBehavior()))//过滤未安装的
                .keyBy(new KeySelector<MarketingUserBehavior, Tuple2<String, String>>() {//用元组作为key
                    @Override
                    public Tuple2<String, String> getKey(MarketingUserBehavior value) throws Exception {
                        return new Tuple2<>(value.getChannel(), value.getBehavior());
                    }
                }).window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new MarketingCountAgg(), new MarketingCountResult());

        //
        aggregateStream.print();
        env.execute("app marketing by channel job");
    }


    public static class MarketingCountAgg implements AggregateFunction<MarketingUserBehavior,Long,Long>{


        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(MarketingUserBehavior value, Long accumulator) {
            return accumulator+1;
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
    public static class MarketingCountResult extends ProcessWindowFunction<Long, ChannelPromotionCount,Tuple2<String,String>, TimeWindow>{



        @Override
        public void process(Tuple2<String, String> stringStringTuple2, Context context, Iterable<Long> elements,
                Collector<ChannelPromotionCount> out) throws Exception {
            String channel = stringStringTuple2.f0;
            String behavior = stringStringTuple2.f1;
            String windowEnd = new Timestamp(context.window().getEnd()).toString();
            Long count = elements.iterator().next();
            out.collect(new ChannelPromotionCount(channel,behavior,windowEnd,count));
        }
    }
}
