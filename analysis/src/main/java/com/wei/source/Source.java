package com.wei.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;

public interface Source {
    //初始化环境
    DataStreamSource<String> initEnv();
}
