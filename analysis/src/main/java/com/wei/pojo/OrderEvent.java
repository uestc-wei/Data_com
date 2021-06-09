package com.wei.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class OrderEvent {

    /**
     * 订单id
     */
    private Long orderId;
    /**
     *事件类型
     */
    private String eventType;
    /**
     * 唯一id
     */
    private String txId;
    /**
     * 时间戳
     */
    private Long timestamp;
}
