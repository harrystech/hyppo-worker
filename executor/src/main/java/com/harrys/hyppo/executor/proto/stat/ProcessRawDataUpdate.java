package com.harrys.hyppo.executor.proto.stat;

import com.harrys.hyppo.executor.ExecutorOperation;
import com.harrys.hyppo.executor.proto.StatusUpdate;

/**
 * Created by jpetty on 7/22/15.
 */
public final class ProcessRawDataUpdate extends StatusUpdate {
    private static final long serialVersionUID = 1L;

    public ProcessRawDataUpdate(){
        super(ExecutorOperation.ProcessRawData);
    }
}
