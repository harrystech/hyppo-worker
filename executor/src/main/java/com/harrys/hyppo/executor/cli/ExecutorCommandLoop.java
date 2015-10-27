package com.harrys.hyppo.executor.cli;

import com.harrys.hyppo.executor.net.CommanderSocketHandler;
import com.harrys.hyppo.executor.net.IPCMessageFrame;
import com.harrys.hyppo.executor.net.WorkerIPCSocket;
import com.harrys.hyppo.executor.proto.ExecutorError;
import com.harrys.hyppo.executor.proto.ExecutorInitMessage;
import com.harrys.hyppo.executor.proto.StartOperationCommand;
import com.harrys.hyppo.executor.proto.init.ExecutorReady;
import com.harrys.hyppo.executor.proto.init.InitializationFailed;
import com.harrys.hyppo.source.api.DataIntegration;
import com.harrys.hyppo.source.api.ProcessedDataIntegration;
import com.harrys.hyppo.source.api.RawDataIntegration;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

/**
 * Created by jpetty on 7/21/15.
 */
public final class ExecutorCommandLoop {

    private final int serverPort;

    private final String className;

    private final ObjectMapper mapper;

    private final TaskSpecificLogging logging;

    private DataIntegration<?> integration;


    public ExecutorCommandLoop(final int serverPort, final String className){
        this.serverPort  = serverPort;
        this.className   = className;
        this.logging     = new TaskSpecificLogging();
        this.integration = null;
        this.mapper      = new ObjectMapper();
        this.mapper.getSerializationConfig().addMixInAnnotations(ExecutorInitMessage.class, ExecutorInitMessage.class);
        this.mapper.getDeserializationConfig().addMixInAnnotations(ExecutorInitMessage.class, ExecutorInitMessage.class);
    }

    public final void runUntilExitCommand() throws Exception {
        while (true){
            try (final WorkerIPCSocket socket = this.connectToCommander()){
                //  Rotate the log files so this task has a dedicated debugging output
                this.logging.rotateTaskLogFile();
                //  Create the handler instance to facilitate this iteration, closes socket at the end
                final StartOperationCommand command = readCommand(socket);
                if (command.isExitCommand()){
                    socket.close();
                    return;
                } else {
                    try {
                        if (this.integration == null){
                            this.initializeIntegration();
                        }
                        new CommanderSocketHandler(mapper, socket, this.integration).handleCommand(command);
                    } catch (Exception e){
                        this.logging.flushLogStream();
                        sendFailureIfPossible(socket, e);
                        throw e;
                    }
                }
            } finally {
                //  Ensure any buffered content in STDOUT is flushed before blocking to reconnect
                this.logging.flushLogStream();
            }
        }
    }

    private final void initializeIntegration() throws InvalidIntegrationClassException {
        try {
            if (this.integration == null){
                this.integration = createIntegrationInstance(this.className);
            }
        } catch (InvalidIntegrationClassException e){
            this.sendInitFailureIfPossible(e);
            throw e;
        }
    }

    private final StartOperationCommand readCommand(final WorkerIPCSocket socket) throws Exception {
        //  Send me work.
        final byte[] content = this.mapper.writeValueAsBytes(new ExecutorReady());
        final IPCMessageFrame ready = IPCMessageFrame.createFromContent(content);
        socket.sendFrame(ready);
        //  Wait for work to do
        final IPCMessageFrame frame = socket.readFrame();
        return this.mapper.readValue(frame.getContent(), StartOperationCommand.class);
    }

    private final void sendFailureIfPossible(final WorkerIPCSocket socket, final Exception e){
        try {
            final ExecutorError error   = ExecutorError.createFromThrowable(e);
            final IPCMessageFrame frame = IPCMessageFrame.createFromContent(mapper.writeValueAsBytes(error));
            if (!socket.isClosed()){
                socket.sendFrame(frame);
            }
        } catch (Exception ee){
            System.err.println("Failed to send failure notification to commander");
            ee.printStackTrace(System.err);
        }
    }

    private final void sendInitFailureIfPossible(final Exception e){
        try (final WorkerIPCSocket socket = this.connectToCommander()){
            final IPCMessageFrame frame = IPCMessageFrame.createFromContent(mapper.writeValueAsBytes(InitializationFailed.createFromThrowable(e)));
            if (!socket.isClosed()){
                socket.sendFrame(frame);
            }
        } catch (Exception ee){
            System.err.println("Failed to connect to commander to send initialization error");
            ee.printStackTrace(System.err);
        }
    }

    private final WorkerIPCSocket connectToCommander() throws IOException {
        return WorkerIPCSocket.connectToCommander(this.serverPort);
    }

    @SuppressWarnings("unchecked")
    public static final DataIntegration<?> createIntegrationInstance(final String className) throws InvalidIntegrationClassException {
        final Class<?> initialClass;
        try {
            initialClass = Class.forName(className);
        } catch (ClassNotFoundException cnf){
            throw new InvalidIntegrationClassException("No class exists with name: " + className, cnf);
        }

        final Class<? extends DataIntegration<?>> castClass;
        if (DataIntegration.class.isAssignableFrom(initialClass)) {
            castClass = (Class<? extends DataIntegration<?>>)initialClass.asSubclass(DataIntegration.class);
        } else {
            throw new InvalidIntegrationClassException(String.format("Class '%s' is not a type of '%s'", className, DataIntegration.class.getName()));
        }

        if (!RawDataIntegration.class.isAssignableFrom(castClass) && !ProcessedDataIntegration.class.isAssignableFrom(castClass)){
            final String msg = String.format("Class '%s' must be either a type of '%s' or '%s'", className, RawDataIntegration.class.getName(), ProcessedDataIntegration.class.getName());
            throw new InvalidIntegrationClassException(msg);
        }

        try {
            return castClass.newInstance();
        } catch (Exception e) {
            throw new InvalidIntegrationClassException("Failed to construct instance of Class: " + className, e);
        }
    }
}
