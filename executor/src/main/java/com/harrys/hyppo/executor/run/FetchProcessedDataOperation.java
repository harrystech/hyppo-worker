package com.harrys.hyppo.executor.run;

import com.harrys.hyppo.executor.net.WorkerIPCSocket;
import com.harrys.hyppo.executor.proto.com.FetchProcessedDataCommand;
import com.harrys.hyppo.executor.proto.res.FetchProcessedDataResult;
import com.harrys.hyppo.source.api.DataIntegration;
import com.harrys.hyppo.source.api.ProcessedDataIntegration;
import com.harrys.hyppo.source.api.ValidationResult;
import com.harrys.hyppo.source.api.data.AvroRecordAppender;
import com.harrys.hyppo.source.api.model.DataIngestionJob;
import com.harrys.hyppo.source.api.model.DataIngestionTask;
import com.harrys.hyppo.source.api.model.IngestionSource;
import com.harrys.hyppo.source.api.task.FetchProcessedData;
import com.harrys.hyppo.source.api.task.ProcessedDataFetcher;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.specific.SpecificRecord;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.nio.file.Files;

/**
 * Created by jpetty on 7/17/15.
 */
public final class FetchProcessedDataOperation extends ExecutorOperation<FetchProcessedDataCommand, FetchProcessedDataResult> {

    private final CodecFactory avroCodec;

    public FetchProcessedDataOperation(
            final FetchProcessedDataCommand command,
            final ObjectMapper mapper,
            final DataIntegration<?> integration,
            final WorkerIPCSocket socket,
            final CodecFactory avroCodec
    ){
        super(command, mapper, integration, socket);
        this.avroCodec = avroCodec;
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

    @SuppressWarnings("unchecked")
    @Override
    public final FetchProcessedDataResult executeForResults() throws Exception {
        final ProcessedDataIntegration<? extends SpecificRecord> integration = this.getProcessedDataIntegration();

        final ValidationResult sourceValidation = integration.validateSourceConfiguration(this.getSource());
        final ValidationResult jobValidation    = integration.validateJobParameters(this.getJob());
        final ValidationResult taskValidation   = integration.validateTaskArguments(this.getTask());
        final ValidationResult validationSum    = sourceValidation.combineWith(jobValidation).combineWith(taskValidation);
        if (validationSum.hasErrors()){
            throw validationSum.toValidationException();
        }

        final ProcessedDataFetcher<? extends SpecificRecord> fetcher = integration.newProcessedDataFetcher();
        if (fetcher == null){
            throw new IllegalArgumentException(String.format("Data Integration '%s' instance created null value for '%s'", this.getIntegrationClass().getName(), ProcessedDataFetcher.class.getName()));
        }

        final File outputFile = Files.createTempFile("ingestion-task-" + this.getJob().getId().toString(), "avro").toFile();
        final AvroRecordAppender<? extends SpecificRecord> appender = integration.avroType().createAvroRecordAppender(outputFile, avroCodec);

        fetcher.fetchProcessedData(new FetchProcessedData(this.getTask(), appender));

        appender.close();

        return new FetchProcessedDataResult(this.getTask(), outputFile, appender.getRecordCount());
    }


}
