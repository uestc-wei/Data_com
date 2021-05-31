package com.wei.util;



import java.io.IOException;
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
     * kafka配置
     *
     */
    public static String getKafkaAddr(){
        if (canGetFromSysArgs("kafkaAddress")) {
            return startUpParameterTool.get("kafkaAddress");
        }
        return parameterTool.get("kafkaAddress");
    }
    public static int getKafkaPort(){
        if (canGetFromSysArgs("kafkaPort")) {
            return startUpParameterTool.getInt("kafkaPort");
        }
        return parameterTool.getInt("kafkaPort");
    }
    public static String getTopic(){
        if (canGetFromSysArgs("topic")) {
            return startUpParameterTool.get("topic");
        }
        return parameterTool.get("topic");
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

    /**
     * es配置
     */
}
