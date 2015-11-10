package com.harrys.hyppo.executor.proto.com;

import com.harrys.hyppo.executor.ExecutorOperation;
import com.harrys.hyppo.executor.proto.StartOperationCommand;
import com.harrys.hyppo.source.api.model.DataIngestionTask;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Created by jpetty on 7/21/15.
 */
public final class FetchProcessedDataCommand extends StartOperationCommand {
    private static final long serialVersionUID = 1L;

    @JsonProperty("task")
    private final DataIngestionTask task;

    @JsonCreator
    public FetchProcessedDataCommand(
            @JsonProperty("task") final DataIngestionTask task
    ){
        super(ExecutorOperation.FetchProcessedData);
        this.task = task;
    }


    public final DataIngestionTask getTask(){
        return this.task;
    }

}
