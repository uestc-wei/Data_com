package com.wei.feature;


import com.wei.pojo.ItemViewCount;
import com.wei.pojo.UserBehavior;
import com.wei.util.DataSourceFactory;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter.Strategy;
import org.apache.flink.util.Collector;

/**
 *统计最近1小时内的热门商品
 *指标：浏览次数PV
 */
public class GetHotItem {

    public static void main(String[] args) {
        //kafkaStringSource
        DataStreamSource<String> source = DataSourceFactory.kafkaStringSourceProduce();
        /**transformation
         * 1.将字符流转成pojo
         * 2.抽取event_time
         */
        SingleOutputStreamOperator<UserBehavior> userBehaviorStream = source.map(line -> {
            String[] split = line.split(",");
            return new UserBehavior(new Long(split[0]), new Long(split[1]),
                    new Integer(split[2]), split[3], new Long(split[4]));
        }).assignTimestampsAndWatermarks(new Strategy<>(
                new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(Time.of(200, TimeUnit.MILLISECONDS)) {
                    @Override
                    public long extractTimestamp(UserBehavior userBehavior) {
                        return userBehavior.getTimestamp() * 1000L;
                    }
                }
        ));

        //分组开窗聚合，得到每个窗口内的各个商品的count值
        SingleOutputStreamOperator<ItemViewCount> windowAggStream = userBehaviorStream
                .filter(userBehavior -> "pv".equals(userBehavior.getBehavior()))//过滤其他行为
                .keyBy(UserBehavior::getItemId) //根据商品id聚合
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))//滑动窗口
                .aggregate(new countItemAgg(), new WindowItemCountResult());

        //TOP n商品
        windowAggStream.keyBy(ItemViewCount::getWindowEnd).process();

    }

    /**
     * 聚合函数
     */
    public static class countItemAgg implements AggregateFunction<UserBehavior,Long,Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long accumulator) {
            return accumulator+1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long accumulator1, Long accumulator2) {
            return accumulator1+accumulator2;
        }
    }
    /**
     * 窗口函数
     */
    public static class WindowItemCountResult
            implements WindowFunction<Long,ItemViewCount,Long, TimeWindow>{

        @Override
        public void apply(Long itemId, TimeWindow timeWindow, Iterable<Long> input,
                Collector<ItemViewCount> out) {
            long end = timeWindow.getEnd();
            Long count = input.iterator().next();
            out.collect(new ItemViewCount(itemId,end,count));
        }
    }
    /**
     * 自定义keyedProcessFunction
     */
     public static class TopNHotItems extends KeyedProcessFunction<Long,ItemViewCount,String>{
         //top n
         private Integer topSize;

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {

        }


        //

    }

}

