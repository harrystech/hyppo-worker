package com.harrys.hyppo.executor.proto.res;

import com.harrys.hyppo.executor.ExecutorOperation;
import com.harrys.hyppo.executor.proto.OperationResult;
import com.harrys.hyppo.source.api.model.DataIngestionTask;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Created by jpetty on 7/22/15.
 */
public final class PersistProcessedDataResult extends OperationResult {
    private static final long serialVersionUID = 1L;

    @JsonProperty("task")
    private final DataIngestionTask task;

    @JsonCreator
    public PersistProcessedDataResult(
            @JsonProperty("task") final DataIngestionTask task
    ) {
        super(ExecutorOperation.PersistProcessedData);
        this.task = task;
    }

    public final DataIngestionTask getTask() {
        return this.task;
    }
}
