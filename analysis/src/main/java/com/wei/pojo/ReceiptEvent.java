package com.wei.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ReceiptEvent {

    /**
     *  唯一对应id
     */
    private String txId;
    /**
     * 支付渠道
     */
    private String payChannel;
    /**
     * 时间戳
     */
    private Long timestamp;
}
