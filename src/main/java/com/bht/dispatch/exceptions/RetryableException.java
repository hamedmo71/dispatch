package com.bht.dispatch.exceptions;

public class RetryableException extends RuntimeException{

    public RetryableException(String message) {
        super(message);
    }

    public RetryableException(Exception exception) {
        super(exception);
    }
}
