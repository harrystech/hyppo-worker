package com.harrys.hyppo.worker.scheduling

/**
  * Created by jpetty on 2/11/16.
  */
trait WorkQueuePrioritizer {
  def prioritize(queues: Seq[WorkQueueMetrics]): Iterator[WorkQueueMetrics]
}

object WorkQueuePrioritizer {

  def createWithPriorities(ordering: List[PriorityOrdering]): WorkQueuePrioritizer = new WorkQueuePrioritizer {
    override def prioritize(queues: Seq[WorkQueueMetrics]): Iterator[WorkQueueMetrics] = {
      recursivePrioritize(ordering, queues.toVector)
    }
  }

  private def recursivePrioritize(chain: List[PriorityOrdering], queues: Vector[WorkQueueMetrics]): Iterator[WorkQueueMetrics] = {
    if (chain.isEmpty || queues.size <= 1) {
      queues.toIterator
    } else {
      val groupIterator = new LocalGroupEqualityIterator(chain.head, queues)
      groupIterator.flatMap { group => recursivePrioritize(chain.tail, group) }
    }
  }


  private final class LocalGroupEqualityIterator
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
        val (group, remainder) = queues.splitAt(findLocalEqualitySize())
        queues = remainder
        group
      }
    }

    private def findLocalEqualitySize(): Int = {
      var index = 1
      val check = queues.head
      while (index < queues.length && priority.compare(check, queues(index)) == 0) {
        index += 1
      }
      index
    }
  }

}
