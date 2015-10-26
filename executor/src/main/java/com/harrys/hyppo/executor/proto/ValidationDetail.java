package com.harrys.hyppo.executor.proto;

import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Created by jpetty on 10/26/15.
 */
public final class ValidationDetail {

    @JsonProperty("message")
    private final String message;

    @JsonProperty("exception")
    private final ExecutorError exception;

    public ValidationDetail(
            @JsonProperty("message") final String message,
            @JsonProperty("exception") final ExecutorError exception
    ){
        this.message = message;
        this.exception = exception;
    }

    public final String getMessage() {
        return message;
    }

    public final ExecutorError getException() {
        return exception;
    }

    public static final ValidationDetail forException(final String message, final Exception e){
        return new ValidationDetail(message, ExecutorError.createFromThrowable(e));
    }
}
