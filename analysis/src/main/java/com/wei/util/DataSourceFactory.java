package com.wei.util;

import com.wei.source.KafkaStringSource;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;


@Data
public class DataSourceFactory {


    private static ParameterTool startUpParameterTool;

    /**
     *
     * @return
     */
    public static DataStreamSource<String> kafkaStringSourceProduce(){
        ConfigUtil.setStartUpParameterTool(startUpParameterTool);
        KafkaStringSource kafkaStringSource =
                new KafkaStringSource();
        return kafkaStringSource.initEnv();
    }

}
