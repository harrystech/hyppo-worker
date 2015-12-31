package com.harrys.hyppo.executor.run;

import com.harrys.hyppo.executor.net.WorkerIPCSocket;
import com.harrys.hyppo.executor.proto.com.ProcessRawDataCommand;
import com.harrys.hyppo.executor.proto.res.ProcessRawDataResult;
import com.harrys.hyppo.source.api.DataIntegration;
import com.harrys.hyppo.source.api.RawDataIntegration;
import com.harrys.hyppo.source.api.ValidationResult;
import com.harrys.hyppo.source.api.data.AvroRecordAppender;
import com.harrys.hyppo.source.api.model.DataIngestionJob;
import com.harrys.hyppo.source.api.model.DataIngestionTask;
import com.harrys.hyppo.source.api.model.IngestionSource;
import com.harrys.hyppo.source.api.task.ProcessRawData;
import com.harrys.hyppo.source.api.task.RawDataProcessor;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.specific.SpecificRecord;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.nio.file.Files;
import java.util.List;

/**
 * Created by jpetty on 7/13/15.
 */
public final class ProcessRawDataOperation extends ExecutorOperation<ProcessRawDataCommand, ProcessRawDataResult> {

    private final CodecFactory avroCodec;

    public ProcessRawDataOperation(
            final ProcessRawDataCommand command,
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

    public final List<File> getLocalRawFiles(){
        return this.getCommand().getLocalRawFiles();
    }

    @SuppressWarnings("unchecked")
    @Override
    public final ProcessRawDataResult executeForResults() throws Exception {
        final RawDataIntegration<? extends SpecificRecord> integration = this.getRawDataIntegration();

        final ValidationResult sourceValidation = integration.validateSourceConfiguration(this.getSource());
        final ValidationResult jobValidation    = integration.validateJobParameters(this.getJob());
        final ValidationResult taskValidation   = integration.validateTaskArguments(this.getTask());
        final ValidationResult validationSum    = sourceValidation.combineWith(jobValidation).combineWith(taskValidation);
        if (validationSum.hasErrors()){
            throw validationSum.toValidationException();
        }

        final String prefix     = "ingestion-job-" + this.getTask().getIngestionJob().getId().toString();
        final File localResults = Files.createTempFile(prefix, "avro").toFile();

        final RawDataProcessor<? extends SpecificRecord> processor  = integration.newRawDataProcessor();
        final AvroRecordAppender<? extends SpecificRecord> appender = integration.avroType().createAvroRecordAppender(localResults, avroCodec);

        for (final File inputFile : this.getLocalRawFiles()){
            //  TODO: Track progress through success / failure
            processor.processRawData(new ProcessRawData(this.getTask(), inputFile, appender));
            appender.flush();
        }

        appender.close();

        return new ProcessRawDataResult(this.getTask(), localResults, appender.getRecordCount());
    }

}
