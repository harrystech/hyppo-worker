package com.harrys.hyppo.executor.proto.res;

import com.harrys.hyppo.executor.OperationType;
import com.harrys.hyppo.executor.proto.OperationResult;
import com.harrys.hyppo.executor.util.SchemaToJson;
import com.harrys.hyppo.source.api.model.IngestionSource;
import org.apache.avro.Schema;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonDeserialize;
import org.codehaus.jackson.map.annotate.JsonSerialize;

/**
 * Created by jpetty on 7/22/15.
 */
public final class ValidateIntegrationResult extends OperationResult {
    private static final long serialVersionUID = 1L;

    @JsonProperty("source")
    private final IngestionSource source;

    @JsonProperty("valid")
    private final boolean isValid;

    @JsonProperty("rawDataIntegration")
    private final boolean isRawDataIntegration;

    @JsonProperty("schema")
    @JsonSerialize(using    = SchemaToJson.Serializer.class)
    @JsonDeserialize(using  = SchemaToJson.Deserializer.class)
    private final Schema schema;

    @JsonCreator
    public ValidateIntegrationResult(
            @JsonProperty("source") final IngestionSource source,
            @JsonProperty("valid")  final boolean isValid,
            @JsonProperty("rawDataIntegration") final boolean isRawDataIntegration,
            @JsonProperty("schema") final Schema schema
    ){
        super(OperationType.ValidateIntegration);
        this.source  = source;
        this.isValid = isValid;
        this.isRawDataIntegration = isRawDataIntegration;
        this.schema  = schema;
    }

    public final IngestionSource getSource(){
        return this.source;
    }

    public final Schema getSchema(){
        return this.schema;
    }

    public final boolean isValid(){
        return this.isValid;
    }

    public final boolean isRawDataIntegration(){
        return this.isRawDataIntegration;
    }
}
