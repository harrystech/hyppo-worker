package com.harrys.hyppo.executor.net;

import com.harrys.hyppo.executor.proto.StartOperationCommand;
import com.harrys.hyppo.executor.proto.com.*;
import com.harrys.hyppo.executor.run.*;
import com.harrys.hyppo.source.api.DataIntegration;
import org.codehaus.jackson.map.ObjectMapper;


/**
 * Created by jpetty on 7/21/15.
 */
public final class CommanderSocketHandler {

    private final WorkerIPCSocket socket;

    private final DataIntegration<?> integration;

    private final ObjectMapper mapper;

    public CommanderSocketHandler(final ObjectMapper mapper, final WorkerIPCSocket socket, final DataIntegration<?> integration){
        this.socket      = socket;
        this.integration = integration;
        this.mapper      = mapper;
    }

    public final void handleCommand(final StartOperationCommand command) throws Exception {
        switch (command.getOperationType()){
            case CreateIngestionTasks:
                this.handleCreateIngestionTasks(assertAndCast(command, CreateIngestionTasksCommand.class));
                break;
            case FetchProcessedData:
                this.handleFetchProcessedData(assertAndCast(command, FetchProcessedDataCommand.class));
                break;
            case FetchRawData:
                this.handleFetchRawData(assertAndCast(command, FetchRawDataCommand.class));
                break;
            case PersistProcessedData:
                this.handlePersistProcessedData(assertAndCast(command, PersistProcessedDataCommand.class));
                break;
            case ProcessRawData:
                this.handleProcessRawData(assertAndCast(command, ProcessRawDataCommand.class));
                break;
            case ValidateIntegration:
                this.handleValidateIntegration(assertAndCast(command, ValidateIntegrationCommand.class));
                break;
            case HandleJobCompleted:
                this.handleJobCompleted(assertAndCast(command, HandleJobCompletedCommand.class));
                break;
            case Exit:
                throw new IllegalArgumentException("Operation: " + command.getOperationType().name() + " is special and should not be passed!");
        }
    }

    private final void handleCreateIngestionTasks(final CreateIngestionTasksCommand command) throws Exception {
        new CreateIngestionTasksOperation(command, mapper, integration, socket).execute();
    }

    private final void handleFetchProcessedData(final FetchProcessedDataCommand command) throws Exception {;
        new FetchProcessedDataOperation(command, mapper, integration, socket).execute();
    }

    private final void handleFetchRawData(final FetchRawDataCommand command) throws Exception {
        new FetchRawDataOperation(command, mapper, integration, socket).execute();
    }


    private final void handlePersistProcessedData(final PersistProcessedDataCommand command) throws Exception {
        new PersistProcessedDataOperation(command, mapper, integration, socket).execute();
    }

    private final void handleProcessRawData(final ProcessRawDataCommand command) throws Exception {
        new ProcessRawDataOperation(command, mapper, integration, socket).execute();
    }

    private final void handleValidateIntegration(final ValidateIntegrationCommand command) throws Exception {
        new ValidateIntegrationOperation(command, mapper, integration, socket).execute();
    }

    private final void handleJobCompleted(final HandleJobCompletedCommand command) throws Exception {
        new HandleJobCompletedOperation(command, mapper, integration, socket).execute();
    }

    private final void sendJsonMessage(final Object message) throws Exception {
        final byte[] messageContent = mapper.writeValueAsBytes(message);
        final IPCMessageFrame frame = IPCMessageFrame.createFromContent(messageContent);
        this.socket.sendFrame(frame);
    }

    private static final <T extends StartOperationCommand> T assertAndCast(final StartOperationCommand command, final Class<T> expect) {
        if (expect.isInstance(command)){
            return expect.cast(command);
        } else {
            throw new IllegalArgumentException("Expected instance of " + expect.getName() + " but received: " + command.getClass().getCanonicalName());
        }
    }
}
