package com.harrys.hyppo.executor.proto;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Created by jpetty on 10/26/15.
 */
public final class ValidationErrorDetail {

    @JsonProperty("message")
    private final String message;

    @JsonProperty("exception")
    private final ExecutorError exception;

    public ValidationErrorDetail(
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

    @JsonIgnore
    public final boolean hasException(){
        return (exception != null);
    }

    public static final ValidationErrorDetail forException(final String message, final Exception e){
        return new ValidationErrorDetail(message, ExecutorError.createFromThrowable(e));
    }
}
