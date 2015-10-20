package com.harrys.hyppo.executor.proto;

import com.harrys.hyppo.executor.OperationType;
import com.harrys.hyppo.executor.proto.com.*;
import org.codehaus.jackson.annotate.*;

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
        @JsonSubTypes.Type(value = CreateIngestionTasksCommand.class,   name = "CreateIngestionTasksCommand"),
        @JsonSubTypes.Type(value = ExitCommand.class,                   name = "ExitCommand"),
        @JsonSubTypes.Type(value = FetchProcessedDataCommand.class,     name = "FetchProcessedDataCommand"),
        @JsonSubTypes.Type(value = FetchRawDataCommand.class,           name = "FetchRawDataCommand"),
        @JsonSubTypes.Type(value = PersistProcessedDataCommand.class,   name = "PersistProcessedDataCommand"),
        @JsonSubTypes.Type(value = ProcessRawDataCommand.class,         name = "ProcessRawDataCommand"),
        @JsonSubTypes.Type(value = ValidateIntegrationCommand.class,    name = "ValidateIntegrationCommand")
})
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class StartOperationCommand implements Serializable {

    protected final OperationType operationType;

    protected StartOperationCommand(final OperationType type){
        this.operationType = type;
    }

    @JsonProperty("operationType")
    public final OperationType getOperationType(){
        return this.operationType;
    }

    @JsonIgnore
    public final boolean isExitCommand(){
        return this.operationType == OperationType.Exit;
    }
}
