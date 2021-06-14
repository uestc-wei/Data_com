package com.wei.util;



import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



@Data
public class ConfigUtil {
    private static Logger logger = LoggerFactory.getLogger(ConfigUtil.class);
    private static ParameterTool parameterTool;
    //flink作业提交时传入的参数
    private static ParameterTool startUpParameterTool;


    static {
        try {
            parameterTool = ParameterTool.fromPropertiesFile(
                    ConfigUtil.class.getClassLoader()
                            .getResourceAsStream("config.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void setStartUpParameterTool(ParameterTool startUpParameterTool) {
        logger.info("set up sys param");
        ConfigUtil.startUpParameterTool = startUpParameterTool;
        logger.info("sys param:{}", ConfigUtil.startUpParameterTool.toMap());
    }


    public static void setStartUpParameterInRuntime(RuntimeContext runtimeContext) {
        startUpParameterTool = ParameterTool.fromMap(
                runtimeContext
                        .getExecutionConfig()
                        .getGlobalJobParameters()
                        .toMap());
    }


    private static boolean canGetFromSysArgs(String argName) {
        return startUpParameterTool != null && StringUtils
                .isNotBlank(startUpParameterTool.get(argName));
    }
    /**
     * 全局配置
     */
    public static Integer getParallelism(){
        if (canGetFromSysArgs("parallelism")){
            return startUpParameterTool.getInt("parallelism");
        }
        return parameterTool.getInt("parallelism");
    }

    /**
     * kafka配置
     *
     */
    public static String getKafkaAddr(){
        if (canGetFromSysArgs("kafkaAddress")) {
            return startUpParameterTool.get("kafkaAddress");
        }
        return parameterTool.get("kafkaAddress");
    }

    //支持传入多个topic
    public static List<String> getTopic(){
        if (canGetFromSysArgs("topic")) {
            return Arrays.asList(startUpParameterTool.get("topic").split(","));
        }
        return Arrays.asList(parameterTool.get("topic").split(","));
    }
    public static String getKeyDeserializer(){
        if (canGetFromSysArgs("key.deserializer")) {
            return startUpParameterTool.get("key.deserializer");
        }
        return parameterTool.get("key.deserializer");
    }
    public static String getValueDeserializer(){
        if (canGetFromSysArgs("value.deserializer")) {
            return startUpParameterTool.get("value.deserializer");
        }
        return parameterTool.get("value.deserializer");
    }
    public static String getAutoOffsetReset(){
        if (canGetFromSysArgs("auto.offset.reset")) {
            return startUpParameterTool.get("auto.offset.reset");
        }
        return parameterTool.get("auto.offset.reset");
    }
    public static String getGroupId(){
        if (canGetFromSysArgs("group.id")){
            return startUpParameterTool.get("group.id");
        }
        return parameterTool.get("group.id");
    }
    public static String getEnableAutoCommit(){
        if (canGetFromSysArgs("enable.auto.commit")){
            return startUpParameterTool.get("enable.auto.commit");
        }
        return parameterTool.get("enable.auto.commit");
    }

    /**
     * es配置
     * todo:
     */
}
