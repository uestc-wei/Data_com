/*
package com.wei.feature.uvCount;

import com.wei.pojo.UserBehavior;
import com.wei.util.DataSourceFactory;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter.Strategy;

public class UniqueUvCount {

    public static void main(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = new DataSourceFactory(parameterTool).kafkaStringSourceProduce(env);

        //
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

        userBehaviorDataStream.filter(data->"uv".equals(data.getBehavior()))

    }
}
*/
