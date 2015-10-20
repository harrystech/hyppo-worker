package com.harrys.hyppo.executor.proto.stat;

import com.harrys.hyppo.executor.OperationType;
import com.harrys.hyppo.executor.proto.StatusUpdate;

/**
 * Created by jpetty on 7/22/15.
 */
public final class FetchProcessedDataUpdate extends StatusUpdate {
    private static final long serialVersionUID = 1L;

    public FetchProcessedDataUpdate(){
        super(OperationType.FetchProcessedData);
    }
}
