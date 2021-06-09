package com.wei.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 用户行为
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class UserBehavior {

    /**
     * uid
     */
    private Long userId;

    /**
     * 商品id
     */
    private Long itemId;

    /**
     * 商品分类id
     */
    private Integer categoryId;

    /**
     * 用户行为
     */
    private String behavior;
    /**
     * 时间戳
     */
    private Long timestamp;
}
