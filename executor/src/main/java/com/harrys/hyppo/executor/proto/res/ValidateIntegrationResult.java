package com.harrys.hyppo.executor.proto.res;

import com.harrys.hyppo.executor.OperationType;
import com.harrys.hyppo.executor.proto.OperationResult;
import com.harrys.hyppo.executor.proto.ValidationDetail;
import com.harrys.hyppo.executor.util.SchemaToJson;
import com.harrys.hyppo.source.api.PersistingSemantics;
import com.harrys.hyppo.source.api.model.IngestionSource;
import org.apache.avro.Schema;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonDeserialize;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.util.List;

/**
 * Created by jpetty on 7/22/15.
 */
public final class ValidateIntegrationResult extends OperationResult {
    private static final long serialVersionUID = 1L;

    @JsonProperty("source")
    private final IngestionSource source;

    @JsonProperty("valid")
    private final boolean isValid;

    @JsonProperty("persistingSemantics")
    private final PersistingSemantics persistingSemantics;

    @JsonProperty("rawDataIntegration")
    private final boolean isRawDataIntegration;

    @JsonProperty("schema")
    @JsonSerialize(using    = SchemaToJson.Serializer.class)
    @JsonDeserialize(using  = SchemaToJson.Deserializer.class)
    private final Schema schema;

    @JsonProperty("validationErrors")
    private final List<ValidationDetail> validationErrors;

    @JsonCreator
    public ValidateIntegrationResult(
            @JsonProperty("source") final IngestionSource source,
            @JsonProperty("persistingSemantics") final PersistingSemantics persistingSemantics,
            @JsonProperty("rawDataIntegration") final boolean isRawDataIntegration,
            @JsonProperty("schema") final Schema schema,
            @JsonProperty("validationErrors") final List<ValidationDetail> validationErrors
    ){
        super(OperationType.ValidateIntegration);
        this.source  = source;
        this.persistingSemantics = persistingSemantics;
        this.isRawDataIntegration = isRawDataIntegration;
        this.schema  = schema;
        this.validationErrors = validationErrors;
        this.isValid = validationErrors.isEmpty();
    }

    public final IngestionSource getSource(){
        return this.source;
    }

    public final Schema getSchema(){
        return this.schema;
    }

    public PersistingSemantics getPersistingSemantics() {
        return persistingSemantics;
    }

    public final boolean isValid(){
        return this.isValid;
    }

    public final boolean isRawDataIntegration(){
        return this.isRawDataIntegration;
    }

    public List<ValidationDetail> getValidationErrors() {
        return validationErrors;
    }
}
