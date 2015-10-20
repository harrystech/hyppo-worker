package com.harrys.hyppo.executor.proto.com;

import com.harrys.hyppo.executor.OperationType;
import com.harrys.hyppo.executor.proto.StartOperationCommand;
import com.harrys.hyppo.source.api.model.IngestionSource;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Created by jpetty on 7/21/15.
 */
public final class ValidateIntegrationCommand extends StartOperationCommand {
    private static final long serialVersionUID = 1L;

    @JsonProperty("source")
    private final IngestionSource source;

    @JsonCreator
    public ValidateIntegrationCommand(
            @JsonProperty("source") final IngestionSource source
    ){
        super(OperationType.ValidateIntegration);
        this.source = source;
    }

    public final IngestionSource getSource(){
        return this.source;
    }
}
