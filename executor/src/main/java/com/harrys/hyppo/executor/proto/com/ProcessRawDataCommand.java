package com.harrys.hyppo.executor.proto.com;

import com.harrys.hyppo.executor.OperationType;
import com.harrys.hyppo.executor.proto.StartOperationCommand;
import com.harrys.hyppo.source.api.model.DataIngestionTask;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.io.File;
import java.util.List;

/**
 * Created by jpetty on 7/21/15.
 */
public final class ProcessRawDataCommand extends StartOperationCommand {
    private static final long serialVersionUID = 1L;

    @JsonProperty("task")
    private final DataIngestionTask task;

    @JsonProperty("localRawFiles")
    private final List<File> localRawFiles;

    @JsonCreator
    public ProcessRawDataCommand(
            @JsonProperty("task")           final DataIngestionTask task,
            @JsonProperty("localRawFiles")  final List<File> localRawFiles
    ){
        super(OperationType.ProcessRawData);
        this.task = task;
        this.localRawFiles = localRawFiles;
    }


    public final DataIngestionTask getTask(){
        return this.task;
    }

    public final List<File> getLocalRawFiles(){
        return this.localRawFiles;
    }
}
