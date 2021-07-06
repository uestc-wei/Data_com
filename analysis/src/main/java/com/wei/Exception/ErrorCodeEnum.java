package com.wei.Exception;

import lombok.Getter;

public enum ErrorCodeEnum {

    States_COMMIT_ERROR(1001,"状态提交后端出错"),

    PRE_COMMIT_ERROR(1101,"预提交出错");

    @Getter
    private int code;

    @Getter
    private String msg;

    ErrorCodeEnum(int code,String msg){
        this.code=code;
        this.msg=msg;
    }
}
