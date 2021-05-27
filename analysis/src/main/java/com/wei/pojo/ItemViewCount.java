package com.wei.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 统计结果
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class ItemViewCount {

    /**
     * 商品id
     */
    private Long itemId;

    /**
     * 窗口结束时间
     */
    private Long windowEnd;
    /**
     * pv数
     */
    private Long count;
}
