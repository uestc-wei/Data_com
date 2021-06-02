package com.wei.feature;


import com.wei.pojo.ItemViewCount;
import com.wei.pojo.UserBehavior;
import com.wei.util.DataSourceFactory;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
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

    public static void main(String[] args) throws Exception {
        //kafkaStringSource
        ParameterTool startUpParameterTool = ParameterTool.fromArgs(args);
        DataStreamSource<String> source = new
                DataSourceFactory(startUpParameterTool).kafkaStringSourceProduce();
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
        DataStream<String> resultStream = windowAggStream.keyBy(ItemViewCount::getWindowEnd)
                .process(new TopNHotItems(5));

        //windowAggStream.keyBy("WindowEnd").process(new TopNHotItems(6));

        //sink
        resultStream.print();

        source.getExecutionEnvironment().execute("hot");
    }

    /**
     * 聚合函数
     */
    public static class countItemAgg implements AggregateFunction<UserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long accumulator1, Long accumulator2) {
            return accumulator1 + accumulator2;
        }
    }

    /**
     * 窗口函数
     */
    public static class WindowItemCountResult
            implements WindowFunction<Long, ItemViewCount, Long, TimeWindow> {

        @Override
        public void apply(Long itemId, TimeWindow timeWindow, Iterable<Long> input,
                Collector<ItemViewCount> out) {
            long end = timeWindow.getEnd();
            Long count = input.iterator().next();
            out.collect(new ItemViewCount(itemId, end, count));
        }
    }

    /**
     * 自定义keyedProcessFunction
     */
    public static class TopNHotItems extends KeyedProcessFunction<Long, ItemViewCount, String> {

        //top n
        private Integer topSize;

        public TopNHotItems(Integer topSize){
            this.topSize=topSize;
        }



        //状态列表，保持当前窗口内所有输出的ItemView
        ListState<ItemViewCount> itemViewCountListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            itemViewCountListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("item-view-count-list", ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            //每来一条数据都存入list中，并且注册定时器
            itemViewCountListState.add(value);
            //设置定时器，窗口结束后1ms触发,这里并不会重复注册，flink底层是通过时间戳区分注册器
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        }

        //定时器触发，对当前窗口的数据进行排序后再输出
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(itemViewCountListState.get().iterator());
            //排序
            itemViewCounts.sort(Comparator.comparingLong(ItemViewCount::getCount));
            StringBuilder resultBuilder = new StringBuilder();
            resultBuilder.append("=================").append(System.lineSeparator());
            resultBuilder.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append(System.lineSeparator());
            //遍历列表,抽取top N 输出
            for (int i = 0; i < Math.min(topSize, itemViewCounts.size()); i++) {
                ItemViewCount currentItemViewCount = itemViewCounts.get(i);
                resultBuilder.append("No ").append(i + 1).append(":")
                        .append(" 商品ID = ").append(currentItemViewCount.getItemId())
                        .append(" 热门度  = ").append(currentItemViewCount.getCount())
                        .append(System.lineSeparator());
            }
            resultBuilder.append("=================").append(System.lineSeparator());

            //控制输出频率
            //Thread.sleep(1000L);
            out.collect(resultBuilder.toString());
        }
    }
}

