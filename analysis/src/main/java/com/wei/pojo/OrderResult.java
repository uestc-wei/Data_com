package com.wei.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 订单结果
 */
@Data
@AllArgsConstructor
public class OrderResult {
    /**
     * 订单id
     */
    private Long orderId;

    /**
     * 结果状态
     */
    private String resultState;
}
