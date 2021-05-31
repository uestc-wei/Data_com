package com.wei.source;


import org.apache.flink.streaming.api.datastream.DataStream;

/**
 *
 * @param <T>
 * 初始化source
 */
public abstract class Source<T> {

    /**
     *
     * @return
     */
    public abstract DataStream<T> initEnv();
}
