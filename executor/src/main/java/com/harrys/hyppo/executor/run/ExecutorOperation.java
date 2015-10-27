package com.harrys.hyppo.executor.run;

import com.harrys.hyppo.executor.net.IPCMessageFrame;
import com.harrys.hyppo.executor.net.WorkerIPCSocket;
import com.harrys.hyppo.executor.proto.OperationResult;
import com.harrys.hyppo.executor.proto.StartOperationCommand;
import com.harrys.hyppo.source.api.DataIntegration;
import com.harrys.hyppo.source.api.ProcessedDataIntegration;
import com.harrys.hyppo.source.api.RawDataIntegration;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

/**
 * Created by jpetty on 7/13/15.
 */
public abstract class ExecutorOperation<C extends StartOperationCommand, R extends OperationResult>  {

    private final DataIntegration<?> integration;

    private final WorkerIPCSocket socket;

    private final ObjectMapper mapper;

    private final C command;

    public ExecutorOperation(final C command, final ObjectMapper mapper, final DataIntegration<?> integration, final WorkerIPCSocket socket){
        this.command     = command;
        this.mapper      = mapper;
        this.integration = integration;
        this.socket      = socket;
    }

    protected final C getCommand(){
        return this.command;
    }

    public final DataIntegration<?> getIntegration(){
        return this.integration;
    }

    public final Class<? extends DataIntegration> getIntegrationClass(){
        return this.integration.getClass();
    }

    public final boolean isRawDataIntegration(){
        return (this.getIntegration() instanceof RawDataIntegration);
    }

    public final RawDataIntegration<?> getRawDataIntegration(){
        if (this.isRawDataIntegration()){
            return (RawDataIntegration<?>)this.getIntegration();
        } else {
            throw new IllegalStateException("Integration class '" + this.getIntegrationClass().getName() + "' is not a type of " + RawDataIntegration.class.getName() + "!");
        }

    }

    public final boolean isProcessedDataIntegration(){
        return (this.getIntegration() instanceof ProcessedDataIntegration);
    }

    public final ProcessedDataIntegration<?> getProcessedDataIntegration(){
        if (this.isProcessedDataIntegration()){
            return (ProcessedDataIntegration<?>)this.getIntegration();
        } else {
            throw new IllegalStateException("Integration class '" + this.getIntegrationClass().getName() + "' is not a type of " + ProcessedDataIntegration.class.getName() + "!");
        }
    }

    protected final void sendRawMessage(final IPCMessageFrame message) throws IOException {
        this.socket.sendFrame(message);
    }

    protected final void sendOperationResult(final OperationResult result) throws IOException {
        final byte[] content = this.mapper.writeValueAsBytes(result);
        this.sendRawMessage(IPCMessageFrame.createFromContent(content));
    }

    private final void sendJsonMessage(final Object value) throws IOException {
        final byte[] content = this.mapper.writeValueAsBytes(value);
        final IPCMessageFrame frame = IPCMessageFrame.createFromContent(content);
        this.sendRawMessage(frame);
    }

    public final void execute() throws Exception {
        //  TODO: Implement exception handler / validation wrapping etc
        final R results = this.executeForResults();
        System.out.flush();
        this.sendOperationResult(results);
    }


    public abstract R executeForResults() throws Exception;

}
