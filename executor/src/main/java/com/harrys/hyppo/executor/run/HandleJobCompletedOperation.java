package com.harrys.hyppo.executor.run;

import com.harrys.hyppo.executor.net.WorkerIPCSocket;
import com.harrys.hyppo.executor.proto.com.HandleJobCompletedCommand;
import com.harrys.hyppo.executor.proto.res.HandleJobCompletedResult;
import com.harrys.hyppo.source.api.DataIntegration;
import com.harrys.hyppo.source.api.task.HandleJobCompleted;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Created by jpetty on 11/9/15.
 */
public final class HandleJobCompletedOperation extends ExecutorOperation<HandleJobCompletedCommand, HandleJobCompletedResult> {

    public HandleJobCompletedOperation(final HandleJobCompletedCommand command, final ObjectMapper mapper, final DataIntegration<?> integration, final WorkerIPCSocket socket){
        super(command, mapper, integration, socket);
    }

    @Override
    public HandleJobCompletedResult executeForResults() throws Exception {
        final DataIntegration<?> integration = this.getIntegration();

        final HandleJobCompletedCommand command = this.getCommand();
        final HandleJobCompleted completed      = new HandleJobCompleted(command.getCompletedAt(), command.getJob(), command.getTasks());

        integration.onJobCompleted(completed);
        return new HandleJobCompletedResult();
    }
}
