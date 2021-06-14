package com.wei.source;

import com.wei.util.ConfigUtil;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;


public class DataSourceFactory {

    private static StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();

    public static StreamExecutionEnvironment getEnv(){
        return env;
    }

    //默认第一个topic创建单流
    public static final Integer INT_ZERO = 0;

    /**
     * 全局配置
     * @param parameterTool
     */
    public static void globalSetting(ParameterTool parameterTool){
        //初始化参数配置
        ConfigUtil.setStartUpParameterTool(parameterTool);
        //设置全局参数
        env.getConfig().setGlobalJobParameters(parameterTool);
        env.setParallelism(ConfigUtil.getParallelism());
    }

    public static Properties kafkaSetting(){
        Properties properties=new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,ConfigUtil.getKafkaAddr());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,ConfigUtil.getGroupId());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,ConfigUtil.getAutoOffsetReset());
        //自动提交偏移量
        //properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,ConfigUtil.getAutoOffsetReset());
        return properties;
    }


    /**
     *
     * @param parameterTool
     * @param clazz
     * @param <T>
     * @return
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public static <T> DataStream<T> createKafkaStream(ParameterTool parameterTool,
            Class<? extends DeserializationSchema> clazz) throws IllegalAccessException, InstantiationException {
        globalSetting(parameterTool);
        Properties properties=new Properties();
        properties.setProperty("bootstrap.servers", ConfigUtil.getKafkaAddr());
        properties.setProperty("auto.offset.reset",   ConfigUtil.getAutoOffsetReset());
        List<String> topicList = ConfigUtil.getTopic();
        //默认以topic列表中的第一个创建单流
        FlinkKafkaConsumer<T> flinkKafkaSource = new FlinkKafkaConsumer(topicList.get(INT_ZERO), clazz.newInstance(), properties);
        return env.addSource(flinkKafkaSource);
    }

    /**
     * 创建kafka多流
     * @param parameterTool
     * @param clazz
     * @param <T>
     * @return
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public static <T> Map<String,DataStream<T>> createMultiKafkaStream(ParameterTool parameterTool,
            Class<? extends DeserializationSchema> clazz) throws IllegalAccessException, InstantiationException {
        globalSetting(parameterTool);
        Properties properties=new Properties();
        properties.setProperty("bootstrap.servers", ConfigUtil.getKafkaAddr());
        properties.setProperty("auto.offset.reset",   ConfigUtil.getAutoOffsetReset());
        List<String> topicList = ConfigUtil.getTopic();
        Map<String,DataStream<T>> dataStreamMap=new HashMap<>();
        for (String topic : topicList) {
            FlinkKafkaConsumer<T> flinkKafkaSource = new FlinkKafkaConsumer(topic, clazz.newInstance(), properties);
            DataStream<T> dataStream = env.addSource(flinkKafkaSource);
            dataStreamMap.put(topic,dataStream);
        }
        return dataStreamMap;
    }
}
