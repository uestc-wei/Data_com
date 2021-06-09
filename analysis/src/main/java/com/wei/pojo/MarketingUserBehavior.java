package com.wei.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 市场营销
 * 用户行为
 */
@AllArgsConstructor
@Data
public class MarketingUserBehavior {

    /**
     * 用户id
     */
    private Long userId;
    /**
     * 行为
     */
    private String behavior;
    /**
     * 渠道
     */
    private String channel;
    /**
     * 时间戳
     */
    private Long timestamp;
}
