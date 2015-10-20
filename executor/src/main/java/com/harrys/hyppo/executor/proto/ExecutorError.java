package com.harrys.hyppo.executor.proto;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jpetty on 9/9/15.
 */
final public class ExecutorError implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("exceptionType")
    private final String exceptionType;

    @JsonProperty("message")
    private final String message;

    @JsonProperty("stackTrace")
    private final List<ExecutorStackFrame> stackTrace;

    @JsonProperty("cause")
    private final ExecutorError cause;

    @JsonCreator
    public ExecutorError(
            @JsonProperty("exceptionType") final String exceptionType,
            @JsonProperty("message") final String message,
            @JsonProperty("stackTrace") final List<ExecutorStackFrame> stackTrace,
            @JsonProperty("cause") final ExecutorError cause
    ) {
        this.exceptionType = exceptionType;
        this.message = message;
        this.stackTrace = stackTrace;
        this.cause = cause;
    }


    public String getExceptionType() {
        return exceptionType;
    }

    public String getMessage() {
        return message;
    }

    public List<ExecutorStackFrame> getStackTrace() {
        return stackTrace;
    }

    public ExecutorError getCause() {
        return cause;
    }

    public static final ExecutorError createFromThrowable(final Throwable cause) {
        final ExecutorError nextCause;
        if (cause.getCause() != null) {
            nextCause = createFromThrowable(cause.getCause());
        } else {
            nextCause = null;
        }
        final StackTraceElement[] trace = cause.getStackTrace();
        final List<ExecutorStackFrame> traceItems = new ArrayList<>(trace.length);
        for (final StackTraceElement ste : cause.getStackTrace()) {
            traceItems.add(ExecutorStackFrame.fromStackTraceElement(ste));
        }
        return new ExecutorError(cause.getClass().getName(), cause.getMessage(), traceItems, nextCause);
    }

    public static final class ExecutorStackFrame {

        @JsonProperty("className")
        private final String className;

        @JsonProperty("methodName")
        private final String methodName;

        @JsonProperty("fileName")
        private final String fileName;

        @JsonProperty("lineNumber")
        private final int lineNumber;

        @JsonCreator
        public ExecutorStackFrame(
                @JsonProperty("className") final String className,
                @JsonProperty("methodName") final String methodName,
                @JsonProperty("fileName") final String fileName,
                @JsonProperty("lineNumber") final int lineNumber
        ) {
            this.className = className;
            this.methodName = methodName;
            this.fileName = fileName;
            this.lineNumber = lineNumber;
        }

        public final String getClassName() {
            return className;
        }

        public final String getMethodName() {
            return methodName;
        }

        public final String getFileName() {
            return fileName;
        }

        public final int getLineNumber() {
            return lineNumber;
        }

        @JsonIgnore
        public final StackTraceElement toStackTraceElement() {
            return new StackTraceElement(this.getClassName(), this.getMethodName(), this.getFileName(), this.getLineNumber());
        }

        public static final ExecutorStackFrame fromStackTraceElement(final StackTraceElement element) {
            return new ExecutorStackFrame(element.getClassName(), element.getMethodName(), element.getFileName(), element.getLineNumber());
        }
    }
}
