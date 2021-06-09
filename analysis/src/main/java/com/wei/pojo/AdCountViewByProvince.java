package com.wei.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 *
 */
@AllArgsConstructor
@Data
public class AdCountViewByProvince {
    /**
     * 省份
     */
    private String province;
    /**
     * 窗口结束时间
     */
    private String windowEnd;
    /**
     *
     */
    private Long count;
}
