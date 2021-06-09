package com.wei.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 页面统计结果
 */
@Data
@AllArgsConstructor
public class PageViewCount {
    /**
     * 页面url
     */
    private String url;
    /**
     * 访问时间
     */
    private Long windowEnd;
    /**
     * 统计数据
     */
    private Long count;
}
