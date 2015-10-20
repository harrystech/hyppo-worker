package com.harrys.hyppo.executor.proto;

import com.harrys.hyppo.executor.proto.init.ExecutorReady;
import com.harrys.hyppo.executor.proto.init.InitializationFailed;
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
        property = "type"
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = ExecutorReady.class,         name = "ExecutorReady"),
        @JsonSubTypes.Type(value = InitializationFailed.class,  name = "InitializationFailed")
})
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class ExecutorInitMessage implements Serializable {

    @JsonProperty("type")
    public final String getType(){
        return this.getClass().getSimpleName();
    }
}
