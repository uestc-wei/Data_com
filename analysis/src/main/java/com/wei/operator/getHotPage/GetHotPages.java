package com.wei.operator.getHotPage;

import com.wei.pojo.ApacheLogEvent;
import com.wei.pojo.PageViewCount;
import com.wei.source.DataSourceFactory;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 统计热门页面
 *统计每分钟的IP访问量，取出IP访问量最大的，5s更新一次
 */
public class GetHotPages {

    public static void main(String[] args) throws Exception{
        ParameterTool startUpParameterTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = DataSourceFactory.getEnv();
        DataStream<String> source = DataSourceFactory.createKafkaStream(startUpParameterTool,
                SimpleStringSchema.class);

        //
        SingleOutputStreamOperator<ApacheLogEvent> loginEventStream = source
                .map(line -> {
                    String[] fields = line.split(",");
                    SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                    long timeStamp = dateFormat.parse(fields[3]).getTime();
                    return new ApacheLogEvent(fields[0],fields[1],timeStamp,fields[5],fields[6]);
                }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<>(
                        new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.of(1, TimeUnit.SECONDS)) {
                            @Override
                            public long extractTimestamp(ApacheLogEvent element) {
                                return element.getTimestamp() * 1000L;
                            }
                        }
                ));
        //定义侧输出流标签
        OutputTag<ApacheLogEvent> lateTag = new OutputTag<ApacheLogEvent>("late"){};

        //分组开窗聚合
        SingleOutputStreamOperator<PageViewCount> aggregateStream = loginEventStream
                .filter(data -> "GET".equals(data.getMethod()))
                .filter(data -> {
                    String regex = "^((?!\\.(css|js|png|ico)$).)*$";
                    return Pattern.matches(regex, data.getUrl());
                })
                .keyBy(ApacheLogEvent::getUrl)
                .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.seconds(5)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(lateTag)
                .aggregate(new PageCountAgg(), new PageCountResult());
        aggregateStream.print("agg");
        aggregateStream.getSideOutput(lateTag).print("late");
        //继续按时间分组，排序输出窗口数据，1分钟刷新一次
        aggregateStream.keyBy(PageViewCount::getWindowEnd)
                .process(new TopNHotPages(5));


        aggregateStream.print();
        env.execute("Hot Ip Job");
    }

    public static class PageCountAgg implements AggregateFunction<ApacheLogEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    public static class PageCountResult implements WindowFunction<Long, PageViewCount, String, TimeWindow> {
        @Override
        public void apply(String url, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out) throws Exception {
            out.collect(new PageViewCount(url, window.getEnd(), input.iterator().next()));
        }
    }

    public static class TopNHotPages extends KeyedProcessFunction<Long,PageViewCount,String>{

        //前几
        private Integer topN;

        public TopNHotPages(Integer topN){
            this.topN=topN;
        }

        //定义map状态保存当前的pageViewCount
        MapState<String,Long> pageViewCountState;

        @Override
        public void open(Configuration parameters) throws Exception {
            pageViewCountState=getRuntimeContext().getMapState(new MapStateDescriptor<String,Long>("page-count-map",String.class,Long.class));
        }

        @Override
        public void processElement(PageViewCount value, Context ctx, Collector<String> out) throws Exception {
            pageViewCountState.put(value.getUrl(),value.getCount());
            //定义一个1分钟的定时器
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd()+60*1000L);
        }


        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //定时器触发
            if (timestamp==ctx.getCurrentKey()+60*1000L){
                pageViewCountState.clear();
                return;
            }
            ArrayList<Map.Entry<String, Long>> pageViewCounts = Lists.newArrayList(pageViewCountState.entries().iterator());
            pageViewCounts.sort(Comparator.comparingLong(Entry::getValue));
            //构造输出字符串
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("=========================").append(System.lineSeparator());
            stringBuilder.append("窗口结束时间：").append(new Timestamp(timestamp-1)).append(System.lineSeparator());
            //取出前topn
            for (int i = 0; i < Math.min(topN,pageViewCounts.size()); i++) {
                Entry<String, Long> pageViewCount  = pageViewCounts.get(i);
                stringBuilder.append("NO ").append(i+1).append(":")
                        .append(" 页面URL = ").append(pageViewCount.getKey())
                        .append(" 浏览量 = ").append(pageViewCount.getValue())
                        .append(System.lineSeparator());
            }
            stringBuilder.append("===============================").append(System.lineSeparator());
            out.collect(stringBuilder.toString());
        }
    }
}
