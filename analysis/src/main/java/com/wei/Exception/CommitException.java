package com.wei.Exception;

public class CommitException extends RuntimeException{

    public CommitException() {
    }

    public CommitException(String message) {
        super(message);
    }

    public CommitException(String message, Throwable cause) {
        super(message, cause);
    }

    public CommitException(Throwable cause) {
        super(cause);
    }

    public CommitException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
