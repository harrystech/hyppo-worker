package com.harrys.hyppo.executor.proto.com;

import com.harrys.hyppo.executor.ExecutorOperation;
import com.harrys.hyppo.executor.proto.StartOperationCommand;
import com.harrys.hyppo.executor.util.LocalDateTimeToJson;
import com.harrys.hyppo.source.api.model.DataIngestionJob;
import com.harrys.hyppo.source.api.model.DataIngestionTask;
import com.harrys.hyppo.source.api.model.TaskAssociations;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonDeserialize;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

/**
 * Created by jpetty on 11/9/15.
 */
public final class HandleJobCompletedCommand extends StartOperationCommand {
    private static final long serialVersionUID = 1L;

    @JsonSerialize(using = LocalDateTimeToJson.Serializer.class)
    @JsonDeserialize(using = LocalDateTimeToJson.Deserializer.class)
    @JsonProperty("completedAt")
    private final LocalDateTime completedAt;

    @JsonProperty("job")
    private final DataIngestionJob job;

    @JsonProperty("tasks")
    private final List<DataIngestionTask> tasks;

    @JsonCreator
    public HandleJobCompletedCommand(
            @JsonProperty("completedAt") final LocalDateTime completedAt,
            @JsonProperty("job")         final DataIngestionJob job,
            @JsonProperty("tasks")       final List<DataIngestionTask> tasks
    ){
        super(ExecutorOperation.HandleJobCompleted);
        this.completedAt = completedAt;
        this.job         = job;
        this.tasks       = Collections.unmodifiableList(TaskAssociations.stripJobReferences(tasks));
    }


    public LocalDateTime getCompletedAt() {
        return completedAt;
    }

    public DataIngestionJob getJob() {
        return job;
    }

    public List<DataIngestionTask> getTasks() {
        return tasks;
    }


}
