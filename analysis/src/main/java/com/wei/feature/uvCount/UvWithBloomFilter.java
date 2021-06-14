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
import redis.clients.jedis.Jedis;

public class UvWithBloomFilter {

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

    public static class MyBloomFilter {

        // 定义位图的大小，一般需要定义为2的整次幂
        private Integer cap;

        public MyBloomFilter(Integer cap) {
            this.cap = cap;
        }

        //实现一个hash函数
        public Long hashCode(String value, Integer seed) {
            Long result = 0L;
            for (int i = 0; i < value.length(); i++) {
                result = result * seed + value.charAt(i);
            }
            return result & (cap - 1);
        }
    }

    // 实现自定义的处理函数
    public static class UvCountResultWithBloomFilter extends
            ProcessAllWindowFunction<UserBehavior, PageViewCount, TimeWindow> {

        // 定义jedis连接和布隆过滤器
        Jedis jedis;
        MyBloomFilter myBloomFilter;

        @Override
        public void open(Configuration parameters) throws Exception {
            jedis = new Jedis("localhost", 6379);
            myBloomFilter = new MyBloomFilter(1 << 29);    // 要处理1亿个数据，用64MB大小的位图
        }

        @Override
        public void process(Context context, Iterable<UserBehavior> elements, Collector<PageViewCount> out)
                throws Exception {
            // 将位图和窗口count值全部存入redis，用windowEnd作为key
            Long windowEnd = context.window().getEnd();
            String bitmapKey = windowEnd.toString();
            // 把count值存成一张hash表
            String countHashName = "uv_count";
            String countKey = windowEnd.toString();

            // 1. 取当前的userId
            Long userId = elements.iterator().next().getUserId();

            // 2. 计算位图中的offset
            Long offset = myBloomFilter.hashCode(userId.toString(), 61);

            // 3. 用redis的getbit命令，判断对应位置的值
            Boolean isExist = jedis.getbit(bitmapKey, offset);

            if (!isExist) {
                // 如果不存在，对应位图位置置1
                jedis.setbit(bitmapKey, offset, true);

                // 更新redis中保存的count值
                Long uvCount = 0L;    // 初始count值
                String uvCountString = jedis.hget(countHashName, countKey);
                if (uvCountString != null && !"".equals(uvCountString)) {
                    uvCount = Long.valueOf(uvCountString);
                }
                jedis.hset(countHashName, countKey, String.valueOf(uvCount + 1));

                out.collect(new PageViewCount("uv", windowEnd, uvCount + 1));
            }
        }

        @Override
        public void close() throws Exception {
            jedis.close();
        }
    }
}