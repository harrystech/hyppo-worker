package com.harrys.hyppo.executor.proto.init;

import com.harrys.hyppo.executor.proto.ExecutorError;
import com.harrys.hyppo.executor.proto.ExecutorInitMessage;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Created by jpetty on 7/21/15.
 */
public final class InitializationFailed extends ExecutorInitMessage {
    private static final long serialVersionUID = 1L;

    @JsonProperty("error")
    private final ExecutorError error;

    @JsonCreator
    public InitializationFailed(
            @JsonProperty("error") final ExecutorError error
    ) {
        this.error = error;
    }

    @JsonProperty("error")
    public final ExecutorError getError(){
        return error;
    }

    public static final InitializationFailed createFromThrowable(final Throwable t) {
        final ExecutorError error = ExecutorError.createFromThrowable(t);
        return new InitializationFailed(error);
    }
}
