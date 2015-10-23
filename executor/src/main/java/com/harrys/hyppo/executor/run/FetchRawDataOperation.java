package com.harrys.hyppo.executor.run;

import com.harrys.hyppo.executor.net.WorkerIPCSocket;
import com.harrys.hyppo.executor.proto.com.FetchRawDataCommand;
import com.harrys.hyppo.executor.proto.res.FetchRawDataResult;
import com.harrys.hyppo.source.api.DataIntegration;
import com.harrys.hyppo.source.api.RawDataIntegration;
import com.harrys.hyppo.source.api.ValidationResult;
import com.harrys.hyppo.source.api.data.RawDataCollector;
import com.harrys.hyppo.source.api.model.DataIngestionJob;
import com.harrys.hyppo.source.api.model.DataIngestionTask;
import com.harrys.hyppo.source.api.model.IngestionSource;
import com.harrys.hyppo.source.api.task.FetchRawData;
import com.harrys.hyppo.source.api.task.RawDataFetcher;
import org.apache.avro.specific.SpecificRecord;
import org.codehaus.jackson.map.ObjectMapper;

import java.nio.file.Files;

/**
 * Created by jpetty on 7/17/15.
 */
public final class FetchRawDataOperation extends ExecutorOperation<FetchRawDataCommand, FetchRawDataResult> {

    public FetchRawDataOperation(final FetchRawDataCommand command, final ObjectMapper mapper, final DataIntegration<?> integration, final WorkerIPCSocket socket){
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


    @Override
    public final FetchRawDataResult executeForResults() throws Exception {
        final RawDataIntegration<? extends SpecificRecord> integration = this.getRawDataIntegration();

        final ValidationResult sourceValidation = integration.validateSourceConfiguration(this.getSource());
        final ValidationResult jobValidation    = integration.validateJobParameters(this.getJob());
        final ValidationResult taskValidation   = integration.validateTaskArguments(this.getTask());
        final ValidationResult validationSum    = sourceValidation.combineWith(jobValidation).combineWith(taskValidation);
        if (validationSum.hasErrors()){
            throw validationSum.toValidationException();
        }

        final RawDataFetcher fetcher = integration.newRawDataFetcher();
        if (fetcher == null){
            throw new IllegalArgumentException(String.format("Data Integration '%s' instance created null value for '%s'", this.getIntegrationClass().getName(), RawDataFetcher.class.getName()));
        }

        final RawDataCollector collector = new RawDataCollector(Files.createTempDirectory("raw-data-" + this.getJob().getId()).toFile());

        final FetchRawData integrationOperation = new FetchRawData(this.getTask(), collector);
        fetcher.fetchRawData(integrationOperation);

        return new FetchRawDataResult(this.getTask(), collector.getRawFiles());
    }
}
