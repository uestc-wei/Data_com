package com.wei.operator.loginFail;

import com.wei.pojo.LoginEvent;
import com.wei.pojo.LoginFailWarning;
import com.wei.source.DataSourceFactory;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;

public class LoginFail {

    public static void main(String[] args) throws Exception {
        ParameterTool startUpParameterTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = DataSourceFactory.getEnv();
        DataStream<String> source = DataSourceFactory.createKafkaStream(startUpParameterTool,
                SimpleStringSchema.class);

        //
        SingleOutputStreamOperator<LoginEvent> loginEventStream = source
                .map(line -> {
                    String[] fields = line.split(",");
                    return new LoginEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<>(
                        new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.of(3, TimeUnit.SECONDS)) {
                            @Override
                            public long extractTimestamp(LoginEvent element) {
                                return element.getTimeStamp() * 1000L;
                            }
                        }
                ));
        //???????????????????????????????????????????????????
        SingleOutputStreamOperator<LoginFailWarning> warningStream = loginEventStream.keyBy(LoginEvent::getUserId)
                .process(new LoginFailDetectWarning(5));
        warningStream.print();
        env.execute("login fail detect job");
    }
    public static class LoginFailDetectWarning extends KeyedProcessFunction<Long, LoginEvent,LoginFailWarning>{
        //????????????????????????
        private Integer maxFailTimes;
        // ?????????????????????2?????????????????????????????????
        ListState<LoginEvent> loginFailEventListState;
        //??????????????????
        ValueState<Long> timerTsState;
        public LoginFailDetectWarning(Integer maxFailTimes){
            this.maxFailTimes = maxFailTimes;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            loginFailEventListState=getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("login-fail-list", LoginEvent.class));
            timerTsState=getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts", Long.class));
        }

        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<LoginFailWarning> out) throws Exception {
            //??????????????????????????????
            if ("fail".equals(value.getLoginState())){
                //1.???????????????????????????????????????
                loginFailEventListState.add(value);
                //???????????????????????????????????????????????????
                if (timerTsState.value()==null){
                    //????????????5???????????????
                    long ts = (value.getTimeStamp() + 5) * 1000L;
                    ctx.timerService().registerEventTimeTimer(ts);
                    timerTsState.update(ts);
                }
            }else {
                //2.????????????????????????????????????
                if (timerTsState.value()!=null){
                    ctx.timerService().deleteEventTimeTimer(timerTsState.value());
                }
                loginFailEventListState.clear();
                timerTsState.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<LoginFailWarning> out) throws Exception {
            //???????????????????????????????????????????????????5??????????????????????????????????????????listState???????????????
            ArrayList<LoginEvent> loginFailEvents = Lists.newArrayList(loginFailEventListState.get().iterator());
            int failTimes = loginFailEvents.size();

            if (failTimes>=maxFailTimes){
                //??????????????????????????????????????????
                out.collect(new LoginFailWarning(ctx.getCurrentKey(),
                        loginFailEvents.get(0).getTimeStamp(),
                        loginFailEvents.get(failTimes-1).getTimeStamp(),
                        "login fail in 5s for " + failTimes + " times"));
            }
            loginFailEventListState.clear();
            timerTsState.clear();
        }
    }
}
