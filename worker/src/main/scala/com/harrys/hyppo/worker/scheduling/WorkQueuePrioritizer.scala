package com.harrys.hyppo.worker.scheduling

/**
  * Created by jpetty on 2/11/16.
  */
trait WorkQueuePrioritizer {
  def prioritize(queues: Seq[WorkQueueMetrics]): Iterator[WorkQueueMetrics]
}

object WorkQueuePrioritizer {

  def withNestedPriorities(ordering: List[PriorityOrdering]): WorkQueuePrioritizer = {
    new LazyRecursiveNestingPriorityOrders(ordering)
  }

  private final class LazyRecursiveNestingPriorityOrders(priorities: List[PriorityOrdering]) extends WorkQueuePrioritizer {

    override def prioritize(queues: Seq[WorkQueueMetrics]): Iterator[WorkQueueMetrics] = {
      recursivePrioritize(priorities, queues.toVector)
    }

    private def recursivePrioritize(chain: List[PriorityOrdering], queues: Vector[WorkQueueMetrics]): Iterator[WorkQueueMetrics] = {
      if (chain.isEmpty || queues.size <= 1) {
        queues.toIterator
      } else {
        val groupIterator = new OrderingGroupPrioritizer(chain.head, queues)
        groupIterator.flatMap { group => recursivePrioritize(chain.tail, group) }
      }
    }
  }

  private final class OrderingGroupPrioritizer
  (
    priority: PriorityOrdering,
    private var queues: Vector[WorkQueueMetrics]
  ) extends Iterator[Vector[WorkQueueMetrics]] {

    queues = queues.sorted(priority)

    override def hasNext: Boolean = queues.nonEmpty

    override def next: Vector[WorkQueueMetrics] = {
      if (queues.isEmpty) {
        throw new NoSuchElementException("No elements are left in iterator!")
      } else {
        val (group, remainder) = queues.splitAt(findLocalEqualitySize(priority, queues))
        queues = remainder
        group
      }
    }
  }


  private def findLocalEqualitySize(priority: PriorityOrdering, queues: Seq[WorkQueueMetrics]): Int = {
    var index = 1
    val check = queues.head
    while (index < queues.length && priority.compare(check, queues(index)) == 0) {
      index += 1
    }
    index
  }
}
