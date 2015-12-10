package com.harrys.hyppo.executor.run;

import com.harrys.hyppo.executor.net.WorkerIPCSocket;
import com.harrys.hyppo.executor.proto.ValidationErrorDetail;
import com.harrys.hyppo.executor.proto.com.ValidateIntegrationCommand;
import com.harrys.hyppo.executor.proto.res.ValidateIntegrationResult;
import com.harrys.hyppo.source.api.*;
import com.harrys.hyppo.source.api.model.IngestionSource;
import com.harrys.hyppo.source.api.task.*;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by jpetty on 7/21/15.
 */
public final class ValidateIntegrationOperation extends ExecutorOperation<ValidateIntegrationCommand, ValidateIntegrationResult> {

    public ValidateIntegrationOperation(final ValidateIntegrationCommand command, final ObjectMapper mapper, final DataIntegration<?> integration, final WorkerIPCSocket socket){
        super(command, mapper, integration, socket);
    }

    public final IngestionSource getSource(){
        return this.getCommand().getSource();
    }

    @Override
    public final ValidateIntegrationResult executeForResults() throws Exception {
        final DataIntegration<? extends SpecificRecord> integration = this.getIntegration();

        //  Combined output of errors
        final List<ValidationErrorDetail> errors = new ArrayList<>();

        boolean isRawData = this.isRawDataIntegration();
        Schema avroSchema = null;
        PersistingSemantics persistingSemantics = null;


        //  Validate that the configuration is understandable to this code
        try {
            final ValidationResult sourceErrors = integration.validateSourceConfiguration(this.getSource());
            errors.addAll(toValidationElements(sourceErrors.getErrors()));
        } catch (Exception e){
            errors.add(ValidationErrorDetail.forException("Failed to validate source configuration", e));
        }

        //  Validate that the avro schema is valid
        try {
            avroSchema = integration.avroType().recordSchema();
            if (avroSchema == null){
                errors.add(new ValidationErrorDetail("Integration returned null avro schema", null));
            }
        } catch (Exception e){
            errors.add(ValidationErrorDetail.forException("Couldn't determine the avro schema", e));
        }

        //  Validate that a task creator can be successfully created
        try {
            if (integration.newIngestionTaskCreator() == null){
                errors.add(new ValidationErrorDetail("Integration returned null " + IngestionTaskCreator.class.getName() + " instance", null));
            }
        } catch (Exception e){
            System.err.println("Failure in " + IngestionTaskCreator.class.getName() + " validation: ");
            e.printStackTrace(System.err);
            errors.add(ValidationErrorDetail.forException("Failed to instantiate a TaskCreator", e));
        }

        //  Validate the persister exists and fetch the persisting semantics
        try {
            final ProcessedDataPersister<? extends SpecificRecord> persister = integration.newProcessedDataPersister();
            if (persister == null){
                errors.add(new ValidationErrorDetail("Integration returned null " + ProcessedDataPersister.class.getName(), null));
            } else {
                persistingSemantics = persister.semantics();
                if (persistingSemantics == null){
                    errors.add(new ValidationErrorDetail(persister.getClass().getName() + " returned null " + PersistingSemantics.class.getName() + "value", null));
                }
            }
        } catch (Exception e){
            System.err.println("Failure in " + ProcessedDataPersister.class.getName() + " validation: ");
            e.printStackTrace(System.err);
            errors.add(ValidationErrorDetail.forException("Integration failed to provide a valid " + ProcessedDataPersister.class.getName(), e));
        }


        if (isRawData){
            validateRawDataIntegration(this.getRawDataIntegration(), errors);
        } else {
            validateProcessedDataIntegration(this.getProcessedDataIntegration(), errors);
        }

        //  Combined output
        return new ValidateIntegrationResult(this.getSource(), persistingSemantics, isRawData, avroSchema, errors);
    }


    private final void validateProcessedDataIntegration(
            final ProcessedDataIntegration<? extends SpecificRecord> integration,
            final List<ValidationErrorDetail> errors) throws Exception {

        //  Validate the processed data fetcher is valid
        try {
            final ProcessedDataFetcher<? extends SpecificRecord> fetcher = integration.newProcessedDataFetcher();
            if (fetcher == null){
                errors.add(new ValidationErrorDetail("Integration returned null " + ProcessedDataFetcher.class.getName(), null));
            }
        } catch (Exception e){
            System.err.println("Failure in " + ProcessedDataFetcher.class.getName() + " validation:");
            e.printStackTrace(System.err);
            errors.add(ValidationErrorDetail.forException("Integration failed to create new " + ProcessedDataFetcher.class.getName(), e));
        }
    }

    private final void validateRawDataIntegration(
            final RawDataIntegration<? extends SpecificRecord> integration,
            final List<ValidationErrorDetail> errors) throws Exception {

        //  Validate the raw data fetcher
        try {
            final RawDataFetcher fetcher = integration.newRawDataFetcher();
            if (fetcher == null){
                errors.add(new ValidationErrorDetail("Integration returned null " + RawDataFetcher.class.getName(), null));
            }
        } catch (Exception e){
            System.err.println("Failure in " + RawDataFetcher.class.getName() + " validation:");
            e.printStackTrace(System.err);
            errors.add(ValidationErrorDetail.forException("Integration failed to create new " + RawDataFetcher.class.getName(), e));
        }

        //  Validate the raw data processor
        try {
            final RawDataProcessor<? extends SpecificRecord> processor = integration.newRawDataProcessor();
            if (processor == null){
                errors.add(new ValidationErrorDetail("Integration returned null " + RawDataProcessor.class.getName(), null));
            }
        } catch (Exception e){
            System.err.println("Failure in " + RawDataProcessor.class.getName() + " validation:");
            e.printStackTrace(System.err);
            errors.add(ValidationErrorDetail.forException("Integration failed to create new " + RawDataProcessor.class.getName(), e));
        }
    }

    private static final List<ValidationErrorDetail> toValidationElements(final String[] messages){
        if (messages.length == 0){
            return Collections.emptyList();
        } else {
            return toValidationElements(Arrays.asList(messages));
        }
    }

    private static final List<ValidationErrorDetail> toValidationElements(final List<String> messages){
        return messages.stream().map((message) -> new ValidationErrorDetail(message, null)).collect(Collectors.toList());
    }
}
