package com.wei.feature.uvCount;

import com.wei.pojo.PageViewCount;
import com.wei.pojo.UserBehavior;
import com.wei.source.DataSourceFactory;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter.Strategy;
import org.apache.flink.util.Collector;
import org.redisson.Redisson;
import org.redisson.api.RHyperLogLog;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;


public class UvWithHyperLogLog {

    public static void main(String[] args) throws Exception {
        ParameterTool startUpParameterTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = DataSourceFactory.getEnv();
        DataStream<String> source = DataSourceFactory.createKafkaStream(startUpParameterTool,
                SimpleStringSchema.class);

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

        SingleOutputStreamOperator<PageViewCount> result = userBehaviorDataStream
                .filter(data -> "pv".equals(data.getBehavior()))
                .timeWindowAll(Time.hours(1))
                .trigger(new MyTrigger())
                .process(new UvCountResultWithBloomFilter());
        result.print();
        env.execute("uv count with bloom filter job");

    }

    public static class MyTrigger extends Trigger<UserBehavior, TimeWindow> {

        @Override
        public TriggerResult onElement(UserBehavior element, long timestamp, TimeWindow window, TriggerContext ctx)
                throws Exception {
            //每一条数据来到，直到触发窗口计算，清空窗口
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        }
    }
    // 实现自定义的处理函数
    public static class UvCountResultWithBloomFilter extends
            ProcessAllWindowFunction<UserBehavior, PageViewCount, TimeWindow> {

        RedissonClient redissonClient;

        @Override
        public void open(Configuration parameters) throws Exception {
            Config config = new Config();
            config.useSingleServer().setAddress("redis://127.0.0.1:6379");
            redissonClient=Redisson.create(config);
        }

        @Override
        public void process(Context context, Iterable<UserBehavior> elements, Collector<PageViewCount> out)
                throws Exception {
            RHyperLogLog<Long> myHyperLogLog = redissonClient.getHyperLogLog("uv");
            Long userId = elements.iterator().next().getUserId();
            myHyperLogLog.add(userId);
            long windowEnd = context.window().getEnd();
            long uvCount = myHyperLogLog.count();
            out.collect(new PageViewCount("uv", windowEnd, uvCount));

        }
    }
}