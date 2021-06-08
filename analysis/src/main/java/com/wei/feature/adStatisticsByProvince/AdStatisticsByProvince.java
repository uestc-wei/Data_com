package com.wei.feature.adStatisticsByProvince;


import com.wei.pojo.AdClickEvent;
import com.wei.pojo.AdCountViewByProvince;
import com.wei.pojo.BlackListUserWarning;
import com.wei.util.DataSourceFactory;
import java.sql.Timestamp;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter.Strategy;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.common.protocol.types.Field.Str;
import scala.Tuple2;

/**
 *
 *根据省份进行分组，创建长度为1小时、滑动距离为5秒的时间窗口进行统计
 *
 */
public class AdStatisticsByProvince {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        DataSourceFactory.init(parameterTool);
        DataStreamSource<String> source = DataSourceFactory.kafkaStringSource(env);

        //
        SingleOutputStreamOperator<AdClickEvent> adClickEventStream = source.map(line -> {
            String[] fields = line.split(",");
            return new AdClickEvent(new Long(fields[0]), new Long(fields[1]), fields[2], fields[3],
                    new Long(fields[4]));
        })
                .assignTimestampsAndWatermarks(new Strategy<>(
                        new BoundedOutOfOrdernessTimestampExtractor<AdClickEvent>(Time.of(200, TimeUnit.MILLISECONDS)) {
                            @Override
                            public long extractTimestamp(AdClickEvent element) {
                                return element.getTimestamp() * 1000L;
                            }
                        }
                ));

        SingleOutputStreamOperator<AdClickEvent> filterAdClickStream  = adClickEventStream
                .keyBy(new KeySelector<AdClickEvent, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> getKey(AdClickEvent value) throws Exception {
                        return new Tuple2<>(value.getUserId(), value.getAdId());
                    }
                }).process(new FilterBlackListUser(100));

        SingleOutputStreamOperator<AdCountViewByProvince> adCountResultStream  = filterAdClickStream
                .keyBy(AdClickEvent::getProvince)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new AdCountAgg(), new AdCountResult());

        adCountResultStream.print();
        filterAdClickStream.getSideOutput(new OutputTag<BlackListUserWarning>("blacklist")).print("blacklist-user");
        env.execute("ad count by province job");
    }
    public static class AdCountAgg implements AggregateFunction<AdClickEvent,Long,Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(AdClickEvent value, Long accumulator) {
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

    public static class AdCountResult implements WindowFunction<Long, AdCountViewByProvince, String, TimeWindow>{

        @Override
        public void apply(String province, TimeWindow window, Iterable<Long> input, Collector<AdCountViewByProvince> out)
                throws Exception {
            String windowEnd = new Timestamp(window.getEnd()).toString();
            Long count = input.iterator().next();
            out.collect(new AdCountViewByProvince(province,windowEnd,count));
        }
    }
    //实现自定义处理函数
    public static class FilterBlackListUser extends KeyedProcessFunction<Tuple2<Long,Long>,AdClickEvent, AdClickEvent>{
        //定义属性：点击次数下线
        private Integer countUpperBound;
        public FilterBlackListUser(Integer countUpperBound){
            this.countUpperBound=countUpperBound;
        }
        //定义状态，保存当前用户对某一广告的点击次数
        ValueState<Long> countState;
        //定义一个标志状态，保存当前用户是否已经是黑名单
        ValueState<Boolean> isSentState;


        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ad-count", Long.class));
            isSentState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-sent", Boolean.class));
        }

        @Override
        public void processElement(AdClickEvent value, Context ctx, Collector<AdClickEvent> out) throws Exception {
            // 判断当前用户对同一广告的点击次数，如果不够上限，该count加1正常输出；
            // 如果到达上限，直接过滤掉，并侧输出流输出黑名单报警
            Long curCount = countState.value();

            Boolean isSent = isSentState.value();
            if (null==curCount){
                curCount=0L;
            }
            if (null==isSent){
                isSent=false;
            }
            //1.判断是否是第一个数据，如果是的话，注册一个第二天0点的定时器
            if (curCount==0){
                long ts = ctx.timerService().currentProcessingTime();
                //注册一天定时器
                long fixedTime = DateUtils.addDays(new Date(ts), 1).getTime();
                ctx.timerService().registerProcessingTimeTimer(fixedTime);
            }
            //2.判断是否报警
            if (curCount>=countUpperBound){
                if (!isSent){
                    //如果没有发送报警
                    isSentState.update(true);
                    ctx.output(new OutputTag<BlackListUserWarning>("blacklist"){},
                            new BlackListUserWarning(value.getUserId(),value.getAdId(),"click over " + countUpperBound + "times."));
                }
                return;
            }
            //如果没有返回，点击次数+1，更新状态，正常输出数据到主流
            countState.update(curCount+1);
            out.collect(value);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickEvent> out) throws Exception {
            countState.clear();
            isSentState.clear();
        }
    }
}
