package com.harrys.hyppo.executor.proto.res;

import com.harrys.hyppo.executor.ExecutorOperation;
import com.harrys.hyppo.executor.proto.OperationResult;
import com.harrys.hyppo.source.api.model.DataIngestionTask;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.io.File;
import java.util.List;

/**
 * Created by jpetty on 7/21/15.
 */
public final class FetchRawDataResult extends OperationResult {
    private static final long serialVersionUID = 1L;

    @JsonProperty("task")
    private final DataIngestionTask task;

    @JsonProperty("rawDataFiles")
    private final List<File> rawDataFiles;

    @JsonCreator
    public FetchRawDataResult(
            @JsonProperty("task") final DataIngestionTask task,
            @JsonProperty("rawDataFiles") final List<File> rawDataFiles
    ) {
        super(ExecutorOperation.FetchRawData);
        this.task = task;
        this.rawDataFiles = rawDataFiles;
    }

    public final DataIngestionTask getTask() {
        return this.task;
    }

    public final List<File> getRawDataFiles() {
        return this.rawDataFiles;
    }
}