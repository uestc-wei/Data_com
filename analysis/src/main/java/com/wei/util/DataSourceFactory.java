package com.wei.util;

import com.wei.source.KafkaStringSource;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import lombok.Data;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;



public class DataSourceFactory {

    public static void init(ParameterTool SPT){
        ConfigUtil.setStartUpParameterTool(SPT);
    }
    /**
     *
     * @param env
     * @return
     */
    public static DataStreamSource<String> kafkaStringSource(StreamExecutionEnvironment env){
        KafkaStringSource kafkaStringSource =
                new KafkaStringSource(new Properties());
        return kafkaStringSource.initEnv(env);
    }

    /**
     *
     * @param env
     * @return
     */
    public static Map<String,DataStreamSource<String>> multiKafkaStringSource(StreamExecutionEnvironment env){
        KafkaStringSource kafkaStringSource =
                new KafkaStringSource(new Properties());
        return kafkaStringSource.initMultiEnv(env);
    }
}
