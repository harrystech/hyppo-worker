package com.harrys.hyppo.executor.run;

import com.harrys.hyppo.executor.net.WorkerIPCSocket;
import com.harrys.hyppo.executor.proto.com.CreateIngestionTasksCommand;
import com.harrys.hyppo.executor.proto.res.CreateIngestionTasksResult;
import com.harrys.hyppo.source.api.DataIntegration;
import com.harrys.hyppo.source.api.ValidationResult;
import com.harrys.hyppo.source.api.model.DataIngestionJob;
import com.harrys.hyppo.source.api.model.DataIngestionTask;
import com.harrys.hyppo.source.api.model.IngestionSource;
import com.harrys.hyppo.source.api.task.CreateTasksForJob;
import com.harrys.hyppo.source.api.task.TaskCreator;
import org.apache.avro.specific.SpecificRecord;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.List;

/**
 * Created by jpetty on 7/13/15.
 */
public final class CreateIngestionTasksOperation extends ExecutorOperation<CreateIngestionTasksCommand, CreateIngestionTasksResult> {

    public CreateIngestionTasksOperation(final CreateIngestionTasksCommand command, final ObjectMapper mapper, final DataIntegration<?> integration, final WorkerIPCSocket socket){
        super(command, mapper, integration, socket);
    }

    public final DataIngestionJob getJob(){
        return this.getCommand().getJob();
    }

    public final IngestionSource getSource(){
        return this.getJob().getIngestionSource();
    }

    @Override
    public final CreateIngestionTasksResult executeForResults() throws Exception {
        final DataIntegration<? extends SpecificRecord> integration = this.getIntegration();

        final ValidationResult sourceValidation = integration.validateSourceConfiguration(this.getSource());
        final ValidationResult jobValidation    = integration.validateJobParameters(this.getJob());

        if (sourceValidation.combineWith(jobValidation).hasErrors()){
            throw sourceValidation.combineWith(jobValidation).toValidationException();
        }

        final TaskCreator creator = integration.newIngestionTaskCreator();
        if (creator == null){
            throw new IllegalArgumentException(String.format("Data Integration '%s' instance created null value for '%s'", this.getIntegrationClass().getName(), TaskCreator.class.getName()));
        }

        final CreateTasksForJob integrationOperation = new CreateTasksForJob(this.getJob());
        creator.createTasks(integrationOperation);

        //  TODO: Validate results are sane
        final List<DataIngestionTask> createdTasks = integrationOperation.getTaskBuilder().build();
        return new CreateIngestionTasksResult(this.getJob(), createdTasks);
    }
}
