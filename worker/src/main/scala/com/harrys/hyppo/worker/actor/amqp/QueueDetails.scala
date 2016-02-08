package com.harrys.hyppo.worker.actor.amqp

import java.time.LocalDateTime

import com.harrys.hyppo.util.TimeUtils

import scala.concurrent.duration._

/**
  * Created by jpetty on 11/9/15.
  */
sealed trait QueueDetails extends Product { self =>
  def size: Int
  def rate: Double
  def ready: Int
  def unacknowledged: Int
  def idleSince: LocalDateTime
  final def isEmpty: Boolean = size <= 0
  final def estimatedCompletionTime: Duration = {
    if (isEmpty){
      Duration.Zero
    } else {
      val speed = this.rate
      if (speed == 0.0){
        Duration.Inf
      } else {
        Duration(size.toDouble / speed, SECONDS)
      }
    }
  }

  override def toString: String = {
    s"${self.productPrefix}(size=$size rate=$rate ready=$ready unacked=$unacknowledged idleSince=$idleSince)"
  }
}

final case class SingleQueueDetails
(
  queueName: String,
  override val size: Int,
  override val rate: Double,
  override val ready: Int,
  override val unacknowledged: Int,
  override val idleSince: LocalDateTime
) extends QueueDetails


final case class MultiQueueDetails
(
  queues: Seq[SingleQueueDetails]
) extends QueueDetails {

  def queueNames: Seq[String] = queues.map(_.queueName)

  def nonEmptyQueues: MultiQueueDetails = {
    MultiQueueDetails(queues.filterNot(_.isEmpty))
  }

  override def size: Int = queues.map(_.size).sum

  override def ready: Int = queues.map(_.ready).sum

  override def unacknowledged: Int = queues.map(_.unacknowledged).sum

  override def rate: Double = {
    if (queues.isEmpty){
      0.0
    } else {
      val sizeSum = this.size.toDouble
      var rateSum = 0.0
      queues.foreach { queue =>
        val weight = queue.size.toDouble / sizeSum
        rateSum   += (queue.rate * weight)
      }
      rateSum
    }
  }

  override def idleSince: LocalDateTime = {
    if (queues.isEmpty){
      TimeUtils.currentLocalDateTime()
    } else {
      implicit val order = Ordering.fromLessThan[LocalDateTime]((one, two) => one.isBefore(two))
      queues.map(_.idleSince).min
    }
  }
}