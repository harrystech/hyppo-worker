package com.harrys.hyppo.executor;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonValue;

/**
 * Created by jpetty on 7/13/15.
 */
public enum OperationType {
    Exit,
    CreateIngestionTasks,
    FetchProcessedData,
    FetchRawData,
    ProcessRawData,
    PersistProcessedData,
    ValidateIntegration;

    @JsonValue
    public final String getName(){
        return this.name();
    }

    @JsonCreator
    public static final OperationType fromName(final String operationName) throws IllegalArgumentException {
        for (OperationType t : OperationType.values()){
            if (t.name().equalsIgnoreCase(operationName)){
                return t;
            }
        }
        throw new IllegalArgumentException(String.format("Unknown operation type: %s", operationName));
    }

    public static final String[] operationNames(){
        final OperationType[] values = OperationType.values();
        final String[] names = new String[values.length];
        for (int i = 0; i < values.length; i++){
            names[i] = values[i].getName();
        }
        return names;
    }
}
