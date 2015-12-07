package com.harrys.hyppo.worker.exec

/**
  * Created by jpetty on 12/7/15.
  */
sealed trait TaskLogStrategy {
  def configName: String
}

object TaskLogStrategy {
  case object PipeTaskLogStrategy extends TaskLogStrategy {
    override def configName: String = "PIPE"
  }

  case object FileTaskLogStrategy extends TaskLogStrategy {
    override def configName: String = "FILE"
  }

  case object NullTaskLogStrategy extends TaskLogStrategy {
    override def configName: String = "NULL"
  }

  final val strategies: Seq[TaskLogStrategy] = Seq(PipeTaskLogStrategy, FileTaskLogStrategy, NullTaskLogStrategy)

  def strategyForName(value: String): TaskLogStrategy = {
    strategies.find(_.configName.equalsIgnoreCase(value)).getOrElse(throw new IllegalArgumentException(s"Unknown TaskLogStrategy: $value. Must be one of: ${ strategies.map(_.configName).mkString(", ") }"))
  }
}

