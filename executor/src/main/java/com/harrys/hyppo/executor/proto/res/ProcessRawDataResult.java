package com.harrys.hyppo.executor.proto.res;

import com.harrys.hyppo.executor.OperationType;
import com.harrys.hyppo.executor.proto.OperationResult;
import com.harrys.hyppo.source.api.model.DataIngestionTask;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.io.File;

/**
 * Created by jpetty on 7/22/15.
 */
public final class ProcessRawDataResult extends OperationResult {
    private static final long serialVersionUID = 1L;

    @JsonProperty("task")
    private final DataIngestionTask task;

    @JsonProperty("localDataFile")
    private final File localDataFile;

    @JsonProperty("recordCount")
    private final long recordCount;

    @JsonCreator
    public ProcessRawDataResult(
            @JsonProperty("task")           final DataIngestionTask task,
            @JsonProperty("localDataFile")  final File localDataFile,
            @JsonProperty("recordCount")    final long recordCount
    ){
        super(OperationType.ProcessRawData);
        this.task = task;
        this.localDataFile = localDataFile;
        this.recordCount   = recordCount;
    }

    public final DataIngestionTask getTask(){
        return this.task;
    }

    public final File getLocalDataFile(){
        return this.localDataFile;
    }

    public final long getRecordCount(){
        return this.recordCount;
    }
}

