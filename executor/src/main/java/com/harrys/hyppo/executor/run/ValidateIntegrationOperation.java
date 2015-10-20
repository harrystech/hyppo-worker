package com.harrys.hyppo.executor.run;

import com.harrys.hyppo.executor.proto.com.ValidateIntegrationCommand;
import com.harrys.hyppo.executor.net.WorkerIPCSocket;
import com.harrys.hyppo.executor.proto.res.ValidateIntegrationResult;
import com.harrys.hyppo.source.api.DataIntegration;
import com.harrys.hyppo.source.api.ValidationResult;
import com.harrys.hyppo.source.api.data.AvroRecordType;
import com.harrys.hyppo.source.api.model.IngestionSource;
import org.apache.avro.specific.SpecificRecord;
import org.codehaus.jackson.map.ObjectMapper;

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

        final ValidationResult sourceValidation = integration.validateSourceConfiguration(this.getSource());
        if (sourceValidation.hasErrors()){
            throw sourceValidation.toValidationException();
        }

        final AvroRecordType<? extends SpecificRecord> avroType = integration.avroType();

        return new ValidateIntegrationResult(this.getSource(), this.isRawDataIntegration(), avroType.recordSchema());
    }
}
