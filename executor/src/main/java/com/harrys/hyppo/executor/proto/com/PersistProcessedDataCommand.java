package com.harrys.hyppo.executor.proto.com;

import com.harrys.hyppo.executor.OperationType;
import com.harrys.hyppo.executor.proto.StartOperationCommand;
import com.harrys.hyppo.source.api.model.DataIngestionTask;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.io.File;

/**
 * Created by jpetty on 7/21/15.
 */
public final class PersistProcessedDataCommand extends StartOperationCommand {
    private static final long serialVersionUID = 1L;

    @JsonProperty("task")
    private final DataIngestionTask task;

    @JsonProperty("localDataFile")
    private final File localDataFile;

    @JsonCreator
    public PersistProcessedDataCommand(
            @JsonProperty("task")           final DataIngestionTask task,
            @JsonProperty("localDataFile")  final File localDataFile
    ){
        super(OperationType.PersistProcessedData);
        this.task = task;
        this.localDataFile = localDataFile;
    }

    public final DataIngestionTask getTask(){
        return this.task;
    }

    public final File getLocalDataFile(){
        return this.localDataFile;
    }
}
