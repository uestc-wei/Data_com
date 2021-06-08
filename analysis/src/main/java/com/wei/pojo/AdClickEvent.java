package com.wei.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 *  广告点击事件
 *
 */
@AllArgsConstructor
@Data
public class AdClickEvent {

    /**
     * 用户id
     */
    private Long userId;
    /**
     * 广告id
     */
    private Long adId;
    /**
     * 省份
     */
    private String province;
    /**
     * 城市
     */
    private String city;
    /**
     * 时间
     */
    private Long timestamp;
}
