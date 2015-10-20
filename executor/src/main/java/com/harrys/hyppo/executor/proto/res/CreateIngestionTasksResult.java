package com.harrys.hyppo.executor.proto.res;

import com.harrys.hyppo.executor.OperationType;
import com.harrys.hyppo.executor.proto.OperationResult;
import com.harrys.hyppo.source.api.model.DataIngestionJob;
import com.harrys.hyppo.source.api.model.DataIngestionTask;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;

/**
 * Created by jpetty on 7/21/15.
 */
public final class CreateIngestionTasksResult extends OperationResult {
    private static final long serialVersionUID = 1L;

    @JsonProperty("job")
    private final DataIngestionJob job;

    @JsonProperty("createdTasks")
    private final List<DataIngestionTask> createdTasks;

    @JsonCreator
    public CreateIngestionTasksResult(
            @JsonProperty("job")            final DataIngestionJob job,
            @JsonProperty("createdTasks")   final List<DataIngestionTask> createdTasks
    ){
        super(OperationType.CreateIngestionTasks);
        this.job = job;
        this.createdTasks = createdTasks;
    }

    public final DataIngestionJob getJob(){
        return this.job;
    }

    public final List<DataIngestionTask> getCreatedTasks(){
        return this.createdTasks;
    }
}
