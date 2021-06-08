package com.wei.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 恶意点击广告
 */
@Data
@AllArgsConstructor
public class BlackListUserWarning {

    /**
     * 用户id
     */
    private Long userId;
    /**
     * 广告id
     */
    private Long adId;
    /**
     * 报警信息
     */
    private String warningMsg;
}
