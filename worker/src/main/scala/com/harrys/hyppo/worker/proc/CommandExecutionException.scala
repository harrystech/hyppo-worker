package com.harrys.hyppo.worker.proc

import java.io.File

import com.harrys.hyppo.executor.proto.StartOperationCommand
import com.harrys.hyppo.worker.api.proto.IntegrationException

/**
 * Created by jpetty on 10/27/15.
 */
final class CommandExecutionException(val command: StartOperationCommand, val error: IntegrationException, val executorLog: Option[File]) extends Exception {

}
