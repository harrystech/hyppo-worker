package com.harrys.hyppo.executor.proto.com;

import com.harrys.hyppo.executor.OperationType;
import com.harrys.hyppo.executor.proto.StartOperationCommand;

/**
 * Created by jpetty on 7/21/15.
 */
public final class ExitCommand extends StartOperationCommand {
    private static final long serialVersionUID = 1L;

    public ExitCommand(){
        super(OperationType.Exit);
    }
}
