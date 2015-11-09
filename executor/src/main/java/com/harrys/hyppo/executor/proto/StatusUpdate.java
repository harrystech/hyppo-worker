package com.harrys.hyppo.executor.proto;

import com.harrys.hyppo.executor.ExecutorOperation;
import com.harrys.hyppo.executor.proto.stat.FetchProcessedDataUpdate;
import com.harrys.hyppo.executor.proto.stat.FetchRawDataUpdate;
import com.harrys.hyppo.executor.proto.stat.PersistProcessedDataUpdate;
import com.harrys.hyppo.executor.proto.stat.ProcessRawDataUpdate;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

import java.io.Serializable;

/**
 * Created by jpetty on 7/22/15.
 */
@JsonTypeInfo(
        use      = JsonTypeInfo.Id.NAME,
        include  = JsonTypeInfo.As.PROPERTY,
        property = "operationType"
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = FetchProcessedDataUpdate.class,      name = "FetchProcessedDataUpdate"),
        @JsonSubTypes.Type(value = FetchRawDataUpdate.class,            name = "FetchRawDataUpdate"),
        @JsonSubTypes.Type(value = PersistProcessedDataUpdate.class,    name = "PersistProcessedDataUpdate"),
        @JsonSubTypes.Type(value = ProcessRawDataUpdate.class,          name = "ProcessRawDataUpdate")
})
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class StatusUpdate implements Serializable {

    private final ExecutorOperation operationType;

    public StatusUpdate(final ExecutorOperation operationType){
        this.operationType = operationType;
    }

    @JsonProperty("operationType")
    public final ExecutorOperation getOperationType(){
        return this.operationType;
    }
}
