package com.harrys.hyppo.worker.proc

import java.io.File

import com.harrys.hyppo.executor.proto.OperationResult

/**
 * Created by jpetty on 10/27/15.
 */
final case class CommandOutput(result: OperationResult, taskLog: Option[File]) {

}
