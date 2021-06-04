package com.wei.source;

import com.wei.util.ConfigUtil;
import java.util.Map;
import java.util.Properties;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * kafka source
 */

@NoArgsConstructor

public class KafkaStringSource {


    /**
     * 初始化kafkaStringSource
     */
    public DataStreamSource<String> initEnv(){
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", ConfigUtil.getKafkaAddr()+":"+ConfigUtil.getKafkaPort());
        properties.setProperty("key.deserializer",  ConfigUtil.getKeyDeserializer());
        properties.setProperty("value.deserializer",  ConfigUtil.getValueDeserializer());
        properties.setProperty("auto.offset.reset",   ConfigUtil.getAutoOffsetReset());
        DataStreamSource<String> kafkaSource = env.addSource(
                new FlinkKafkaConsumer<>(ConfigUtil.getTopic(),
                        new SimpleStringSchema(), properties));
        return kafkaSource;
    }
}
