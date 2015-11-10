package com.harrys.hyppo.executor.proto.res;

import com.harrys.hyppo.executor.ExecutorOperation;
import com.harrys.hyppo.executor.proto.OperationResult;

/**
 * Created by jpetty on 11/9/15.
 */
public final class HandleJobCompletedResult extends OperationResult {
    private static final long serialVersionUID = 1L;

    public HandleJobCompletedResult(){
        super(ExecutorOperation.HandleJobCompleted);
    }
}
