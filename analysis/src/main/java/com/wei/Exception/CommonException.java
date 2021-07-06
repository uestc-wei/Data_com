package com.wei.Exception;


import lombok.Data;

@Data
public class CommonException extends Exception{
    private int code;
    private String message;

    public CommonException(int code,String message){
        this.code=code;
        this.message=message;
    }

    public CommonException(ErrorCodeEnum errorCodeEnum){
        this.code=errorCodeEnum.getCode();
        this.message=errorCodeEnum.getMsg();
    }
}
