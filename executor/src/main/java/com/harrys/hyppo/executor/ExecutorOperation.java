package com.harrys.hyppo.executor;

import com.harrys.hyppo.source.api.LogicalOperation;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonValue;

import java.util.Optional;

/**
 * Created by jpetty on 7/13/15.
 */
public enum ExecutorOperation {
    Exit(Optional.empty()),
    CreateIngestionTasks(LogicalOperation.CreateIngestionTasks),
    FetchProcessedData(LogicalOperation.FetchProcessedData),
    FetchRawData(LogicalOperation.FetchRawData),
    ProcessRawData(LogicalOperation.ProcessRawData),
    PersistProcessedData(LogicalOperation.PersistProcessedData),
    ValidateIntegration(LogicalOperation.ValidateIntegration),
    HandleJobCompleted(LogicalOperation.HandleJobCompleted);

    private final Optional<LogicalOperation> logicalOperation;

    private ExecutorOperation(final LogicalOperation logicalOperation){
        this.logicalOperation = Optional.of(logicalOperation);
    }

    private ExecutorOperation(final Optional<LogicalOperation> logicalOperation){
        this.logicalOperation = logicalOperation;
    }

    @JsonValue
    public final String getName(){
        return this.name();
    }

    public final Optional<LogicalOperation> getLogicalOperation(){
        return this.logicalOperation;
    }

    @JsonCreator
    public static final ExecutorOperation fromName(final String operationName) throws IllegalArgumentException {
        for (ExecutorOperation t : ExecutorOperation.values()){
            if (t.name().equalsIgnoreCase(operationName)){
                return t;
            }
        }
        throw new IllegalArgumentException(String.format("Unknown operation type: %s", operationName));
    }

    public static final ExecutorOperation fromLogicalOperation(final LogicalOperation operation) {
        for (ExecutorOperation t : ExecutorOperation.values()){
            if (t.logicalOperation.isPresent() && t.logicalOperation.get() == operation){
                return t;
            }
        }
        throw new IllegalArgumentException(String.format("No matching %s for %s: %s", ExecutorOperation.class.getSimpleName(), LogicalOperation.class.getSimpleName(), operation.name()));
    }

    public static final String[] operationNames(){
        final ExecutorOperation[] values = ExecutorOperation.values();
        final String[] names = new String[values.length];
        for (int i = 0; i < values.length; i++){
            names[i] = values[i].getName();
        }
        return names;
    }
}
