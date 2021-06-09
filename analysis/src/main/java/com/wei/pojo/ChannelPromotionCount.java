package com.wei.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 渠道推广统计
 */
@Data
@AllArgsConstructor
public class ChannelPromotionCount {

    /**
     * 渠道
     */
    private String channel;
    /**
     * 行为
     */
    private String behavior;
    /**
     * 窗口时间
     */
    private String windowEnd;
    /**
     * count
     */
    private Long count;
}
