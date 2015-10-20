package com.harrys.hyppo.executor.proto.com;

import com.harrys.hyppo.executor.OperationType;
import com.harrys.hyppo.executor.proto.StartOperationCommand;
import com.harrys.hyppo.source.api.model.DataIngestionJob;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Created by jpetty on 7/21/15.
 */
public final class CreateIngestionTasksCommand extends StartOperationCommand {
    private static final long serialVersionUID = 1L;

    @JsonProperty("job")
    private final DataIngestionJob job;

    @JsonCreator
    public CreateIngestionTasksCommand(
            @JsonProperty("job") final DataIngestionJob job
    ){
        super(OperationType.CreateIngestionTasks);
        this.job = job;
    }


    public final DataIngestionJob getJob(){
        return this.job;
    }
}
