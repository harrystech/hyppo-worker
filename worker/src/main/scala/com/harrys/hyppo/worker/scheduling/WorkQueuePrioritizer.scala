package com.harrys.hyppo.worker.scheduling

import com.harrys.hyppo.worker.actor.amqp.SingleQueueDetails

/**
  * Created by jpetty on 2/11/16.
  */
trait WorkQueuePrioritizer {
  def prioritize(queues: Seq[SingleQueueDetails]): Iterator[SingleQueueDetails]
}

object WorkQueuePrioritizer {

  def withNestedPriorities(ordering: PriorityOrdering, nested: PriorityOrdering*): WorkQueuePrioritizer = {
    new LazyRecursiveNestingPriorityOrders(List(ordering) ++ nested)
  }

  private final class LazyRecursiveNestingPriorityOrders(priorities: List[PriorityOrdering]) extends WorkQueuePrioritizer {

    override def prioritize(queues: Seq[SingleQueueDetails]): Iterator[SingleQueueDetails] = {
      recursivePrioritize(priorities, queues.toVector)
    }

    private def recursivePrioritize(chain: List[PriorityOrdering], queues: Vector[SingleQueueDetails]): Iterator[SingleQueueDetails] = {
      if (queues.size <= 1) {
        queues.toIterator
      } else {
        val groupIterator = new OrderingGroupPrioritizer(chain.head, queues)
        groupIterator.flatMap { group =>
          if (chain.tail.isEmpty) {
            // The last ordering can be applied in-place once much more readily and without further recursion
            group.sorted(chain.head)
          } else {
            //  Non-terminal prioritizing functions may be split further and need to force lazy evaluation
            // within sub-groups
            recursivePrioritize(chain.tail, group)
          }
        }
      }
    }
  }

  private final class OrderingGroupPrioritizer
  (
    priority: PriorityOrdering,
    private var queues: Vector[SingleQueueDetails]
  ) extends Iterator[Vector[SingleQueueDetails]] {

    queues = queues.sorted(priority)

    override def hasNext: Boolean = queues.nonEmpty

    override def next: Vector[SingleQueueDetails] = {
      if (queues.isEmpty) {
        throw new NoSuchElementException("No elements are left in iterator!")
      } else {
        val (group, remainder) = queues.splitAt(findLocalEqualitySize(queues))
        queues = remainder
        group
      }
    }

    private def findLocalEqualitySize(queues: Vector[SingleQueueDetails]): Int = {
      var index = 1
      val check = queues.head
      while (index < queues.length && priority.compare(check, queues(index)) == 0) {
        index += 1
      }
      index
    }
  }
}
