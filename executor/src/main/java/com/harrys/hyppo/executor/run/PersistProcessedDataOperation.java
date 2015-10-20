package com.harrys.hyppo.executor.run;

import com.harrys.hyppo.executor.proto.com.PersistProcessedDataCommand;
import com.harrys.hyppo.executor.net.WorkerIPCSocket;
import com.harrys.hyppo.executor.proto.res.PersistProcessedDataResult;
import com.harrys.hyppo.source.api.DataIntegration;
import com.harrys.hyppo.source.api.ValidationResult;
import com.harrys.hyppo.source.api.model.DataIngestionJob;
import com.harrys.hyppo.source.api.model.DataIngestionTask;
import com.harrys.hyppo.source.api.model.IngestionSource;
import com.harrys.hyppo.source.api.task.PersistProcessedData;
import com.harrys.hyppo.source.api.task.ProcessedDataPersister;
import org.apache.avro.specific.SpecificRecord;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;

/**
 * Created by jpetty on 7/13/15.
 */
public final class PersistProcessedDataOperation extends ExecutorOperation<PersistProcessedDataCommand, PersistProcessedDataResult> {

    public PersistProcessedDataOperation(final PersistProcessedDataCommand command, final ObjectMapper mapper, final DataIntegration<?> integration, final WorkerIPCSocket socket){
        super(command, mapper, integration, socket);
    }

    public final DataIngestionTask getTask(){
        return this.getCommand().getTask();
    }

    public final DataIngestionJob getJob(){
        return this.getTask().getIngestionJob();
    }

    public final IngestionSource getSource(){
        return this.getJob().getIngestionSource();
    }

    public final File getLocalDataFile(){
        return this.getCommand().getLocalDataFile();
    }

    @SuppressWarnings("unchecked")
    @Override
    public final PersistProcessedDataResult executeForResults() throws Exception {
        final DataIntegration<? extends SpecificRecord> integration = this.getIntegration();

        final ValidationResult sourceValidation = integration.validateSourceConfiguration(this.getSource());
        final ValidationResult jobValidation    = integration.validateJobParameters(this.getJob());
        final ValidationResult taskValidation   = integration.validateTaskArguments(this.getTask());
        final ValidationResult validationSum    = sourceValidation.combineWith(jobValidation).combineWith(taskValidation);
        if (validationSum.hasErrors()){
            throw validationSum.toValidationException();
        }

        final ProcessedDataPersister<? extends SpecificRecord> persister = integration.newDataPersister();
        persister.persistProcessedData(new PersistProcessedData(this.getTask(), integration.avroType(), this.getLocalDataFile()));

        return new PersistProcessedDataResult(this.getTask());
    }
}
