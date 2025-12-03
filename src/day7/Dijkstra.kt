package day7

import java.util.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.*

private val NODE_DISTANCE_COMPARATOR = Comparator<Node> { o1, o2 -> Integer.compare(o1!!.distance, o2!!.distance) }

class MultiQueuePriority<T>(
    workers: Int,
    private val comparator: Comparator<T>,
    factor: Int = 3
    ) {

    private val nOfQueues = factor * workers
    private val queues = Array(nOfQueues) { PriorityQueue(comparator) }
    private val lockArray = Array(nOfQueues) { ReentrantLock() }

    fun add(task: T) {
        while(true) {
            val i = ThreadLocalRandom.current().nextInt(nOfQueues)
            if(!lockArray[i].tryLock()) continue
            queues[i].add(task)
            lockArray[i].unlock()
            return
        }
    }

    fun poll(): T? {
        while(true) {
            val i1 = ThreadLocalRandom.current().nextInt(nOfQueues)
            val i2 = ThreadLocalRandom.current().nextInt(nOfQueues)
            if (i1 == i2) continue

            val (first, second) = if (i1 < i2) i1 to i2 else i2 to i1

            if (!lockArray[first].tryLock()) continue
            if (!lockArray[second].tryLock()) {
                lockArray[first].unlock()
                continue
            }

            val q1 = queues[i1]
            val q2 = queues[i2]

            val q1Top = q1.peek()
            val q2Top = q2.peek()

            val result = when {
                q1Top == null && q2Top == null -> null
                q1Top == null -> q2.poll()
                q2Top == null -> q1.poll()
                comparator.compare(q1Top, q2Top) <= 0 -> q1.poll()
                else -> q2.poll()
            }

            lockArray[second].unlock()
            lockArray[first].unlock()

            return result
        }
    }
}

// Returns `Integer.MAX_VALUE` if a path has not been found.
fun shortestPathParallel(start: Node) {
    val workers = Runtime.getRuntime().availableProcessors()
    // The distance to the start node is `0`
    start.distance = 0
    // Create a priority (by distance) queue and add the start node into it
    val q = MultiQueuePriority(workers, NODE_DISTANCE_COMPARATOR)
    q.add(start)
    // Run worker threads and wait until the total work is done
    val onFinish = Phaser(workers + 1) // `arrive()` should be invoked at the end by each worker
    val activeNodes = AtomicInteger(1)
    repeat(workers) {
        thread {
            while (true) {
                val currentNode: Node = q.poll() ?: if (activeNodes.get() == 0) break else continue

                for (e in currentNode.outgoingEdges) {
                    while (true) {
                        val currentDist = e.to.distance
                        val newDist = currentNode.distance + e.weight
                        if (currentDist <= newDist) break
                        if (e.to.casDistance(currentDist, newDist)) {
                            activeNodes.incrementAndGet()
                            q.add(e.to)
                            break
                        }
                    }
                }
                activeNodes.decrementAndGet()
            }
            onFinish.arrive()
        }
    }
    onFinish.arriveAndAwaitAdvance()
}