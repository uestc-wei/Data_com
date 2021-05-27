package com.wei.source;

import com.wei.util.ConfigUtil;
import java.util.Map;
import java.util.Properties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * kafka source
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class KafkaSource implements Source {

    /**
     * 初始化kafkasource
     */
    public DataStreamSource<String> initEnv(){
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        ConfigUtil configure = new ConfigUtil();
        configure.init();
        Map<String, String> configureInfo = configure.getConfigureInfo();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", configureInfo.get("addr")+":"+configureInfo.get("kafka.port"));
        properties.setProperty("key.deserializer", configureInfo.get("key.deserializer"));
        properties.setProperty("value.deserializer", configureInfo.get("value.deserializer"));
        properties.setProperty("auto.offset.reset", configureInfo.get("auto.offset.reset"));
        env.setParallelism(Integer.parseInt(configureInfo.get("parallelism")));
        DataStreamSource<String> kafkaSource = env.addSource(
                new FlinkKafkaConsumer<String>(configureInfo.get("topic"),
                        new SimpleStringSchema(), properties));
        return kafkaSource;

    }
}
