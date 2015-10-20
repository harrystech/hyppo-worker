package com.harrys.hyppo.executor.net;

/**
 * Created by jpetty on 7/9/15.
 */
public class InvalidMessageFrameException extends Exception {

    public InvalidMessageFrameException(String message){
        super(message);
    }

    public InvalidMessageFrameException(String message, Throwable cause){
        super(message, cause);
    }
}
