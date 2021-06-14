package com.wei.feature.loginFail;

import com.wei.pojo.LoginEvent;
import com.wei.pojo.LoginFailWarning;
import com.wei.source.DataSourceFactory;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;

public class LoginFailCep {

    public static void main(String[] args) throws Exception{
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


        //1.定义一个匹配模式
        Pattern<LoginEvent, LoginEvent> failPattern = Pattern.<LoginEvent>begin("firstFail")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "fail".equals(value.getLoginState());
                    }
                }).times(3).consecutive().within(Time.seconds(5));

        //2.将匹配模式应用到数据流上，得到pattern stream
        PatternStream<LoginEvent> patternStream = CEP.pattern(loginEventStream.keyBy(LoginEvent::getUserId), failPattern);

        //3.检出符合匹配条件的复杂事件
        SingleOutputStreamOperator<LoginFailWarning> warningStream = patternStream.select(new LoginFailMatchDetectWarning());
        warningStream.print();
        env.execute("login fail detect job");

    }

    public static class LoginFailMatchDetectWarning implements PatternSelectFunction<LoginEvent, LoginFailWarning>{

        @Override
        public LoginFailWarning select(Map<String, List<LoginEvent>> pattern) throws Exception {
            LoginEvent firstEvents = pattern.get("failEvents").get(0);
            LoginEvent lastEvents = pattern.get("failEvents").get(pattern.get("failEvents").size()-1);
            return new LoginFailWarning(firstEvents.getUserId(),firstEvents.getTimeStamp(),lastEvents.getTimeStamp(),
                    "login fail " + pattern.get("failEvents").size() + " times");
        }
    }
}
