package com.harrys.hyppo.executor.proto;

import com.harrys.hyppo.executor.ExecutorOperation;
import com.harrys.hyppo.executor.proto.res.*;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

import java.io.Serializable;

/**
 * Created by jpetty on 7/21/15.
 */
@JsonTypeInfo(
        use      = JsonTypeInfo.Id.NAME,
        include  = JsonTypeInfo.As.PROPERTY,
        property = "operationType"
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = CreateIngestionTasksResult.class,    name = "CreateIngestionTasksResult"),
        @JsonSubTypes.Type(value = FetchProcessedDataResult.class,      name = "FetchProcessedDataResult"),
        @JsonSubTypes.Type(value = FetchRawDataResult.class,            name = "FetchRawDataResult"),
        @JsonSubTypes.Type(value = PersistProcessedDataResult.class,    name = "PersistProcessedDataResult"),
        @JsonSubTypes.Type(value = ProcessRawDataResult.class,          name = "ProcessRawDataResult"),
        @JsonSubTypes.Type(value = ValidateIntegrationResult.class,     name = "ValidateIntegrationResult"),
        @JsonSubTypes.Type(value = HandleJobCompletedResult.class,      name = "HandleJobCompletedResult")
})
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class OperationResult implements Serializable {

    private final ExecutorOperation operationType;

    protected OperationResult(final ExecutorOperation operationType){
        this.operationType = operationType;
    }

    @JsonProperty("operationType")
    public final ExecutorOperation getOperationType(){
        return this.operationType;
    }
}
