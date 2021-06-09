package com.wei.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 访问日志信息
 */
@Data
@AllArgsConstructor
public class ApacheLogEvent {

    /**
     * ip
     */
    private String ip;
    /**
     * uid
     */
    private String userId;
    /**
     * time
     */
    private Long timestamp;
    /**
     *访问方法
     */
    private String method;
    /**
     *访问链接
     */
    private String url;
}
