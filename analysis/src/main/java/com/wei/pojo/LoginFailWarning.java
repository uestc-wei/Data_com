package com.wei.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class LoginFailWarning {

    /**
     * 用户id
     */
    private Long userId;
    /**
     *第一次登录失败时间
     */
    private Long firstFailTime;
    /**
     * 最后失败时间
     */
    private Long lastFailTime;
    /**
     * 报警信息
     */
    private String warningMsg;


}
