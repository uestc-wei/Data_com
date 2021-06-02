package com.wei.util;

import com.wei.source.KafkaStringSource;
import lombok.Data;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;


@Data
public class DataSourceFactory {

    private ParameterTool startUpParameterTool;

    public DataSourceFactory(ParameterTool startUpParameterTool){
        this.startUpParameterTool=startUpParameterTool;
        ConfigUtil.setStartUpParameterTool(startUpParameterTool);
    }
    /**
     *
     * @return
     */
    public DataStreamSource<String> kafkaStringSourceProduce(){
        KafkaStringSource kafkaStringSource =
                new KafkaStringSource();
        return kafkaStringSource.initEnv();
    }

}
