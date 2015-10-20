package com.harrys.hyppo.executor.cli;

/**
 * Created by jpetty on 7/21/15.
 */
public class InvalidIntegrationClassException extends Exception {

    public InvalidIntegrationClassException(){
        super();
    }

    public InvalidIntegrationClassException(final String message){
        super(message);
    }

    public InvalidIntegrationClassException(final String message, final Throwable cause){
        super(message, cause);
    }
}
