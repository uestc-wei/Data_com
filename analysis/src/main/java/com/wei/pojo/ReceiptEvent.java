package com.wei.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ReceiptEvent {

    /**
     *
     */
    private String txId;
    /**
     *
     */
    private String payChannel;
    /**
     *
     */
    private Long timestamp;
}
