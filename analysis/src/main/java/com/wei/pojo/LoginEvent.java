package com.wei.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 登录日志
 */
@AllArgsConstructor
@Data
public class LoginEvent {

    /**
     * uid
     */
    private Long userId;
    /**
     * 登录ip
     */
    private String ip;
    /**
     * 登录状态
     */
    private String loginState;
    /**
     * 时间戳
     */
    private Long timeStamp;

}
