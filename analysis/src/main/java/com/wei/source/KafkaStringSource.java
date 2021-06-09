package com.wei.source;

import com.wei.util.ConfigUtil;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import lombok.AllArgsConstructor;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * kafka source
 */


@AllArgsConstructor
public class KafkaStringSource {
    private static Logger logger = LoggerFactory.getLogger(KafkaStringSource.class);
    private Properties properties;
    public void init(){
        properties.setProperty("bootstrap.servers", ConfigUtil.getKafkaAddr());
        properties.setProperty("key.deserializer",  ConfigUtil.getKeyDeserializer());
        properties.setProperty("value.deserializer",  ConfigUtil.getValueDeserializer());
        properties.setProperty("auto.offset.reset",   ConfigUtil.getAutoOffsetReset());
    }

    /**
     * 初始化单流 kafkaStringSource
     * 默认第一个
     */
    public DataStreamSource<String> initEnv(StreamExecutionEnvironment env){
        init();
        logger.info("当前输入流topic："+ Arrays.toString(ConfigUtil.getTopic()));
        DataStreamSource<String> kafkaSource = env.addSource(
                new FlinkKafkaConsumer<>(ConfigUtil.getTopic()[0],
                        new SimpleStringSchema(), properties));
        return kafkaSource;
    }
    /**
     * 初始化多流 kafkaSource
     */
    public Map<String,DataStreamSource<String>> initMultiEnv(StreamExecutionEnvironment env){
        init();
        logger.info("当前输入流topic："+ Arrays.toString(ConfigUtil.getTopic()));
        Map<String,DataStreamSource<String>> dataStreamSourceMap = new HashMap<>();
        String[] topics = ConfigUtil.getTopic();
        for (String topic : topics) {
            DataStreamSource<String> source = env
                    .addSource(new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties));
            dataStreamSourceMap.put(topic,source);
        }
        return dataStreamSourceMap;
    }
}
