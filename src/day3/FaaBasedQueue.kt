package day3

import day2.*
import java.util.concurrent.atomic.*

class FAABasedQueue<E> : Queue<E> {
    private val enqIndex = AtomicLong(0)
    private val deqIndex = AtomicLong(0)

    private val head: AtomicReference<Segment>
    private val tail: AtomicReference<Segment>

    private object Tombstone

    init {
        val startSegment = Segment(0)
        head = AtomicReference(startSegment)
        tail = AtomicReference(startSegment)
    }

    private fun findSegment(start: Segment, targetId: Long): Segment {
        var current = start
        while (current.id < targetId) {
            val next = current.next.get()
            current = if (next == null) {
                val newSegment = Segment(current.id + 1)
                if (current.next.compareAndSet(null, newSegment)) newSegment else current.next.get()!!
            } else next
        }
        return current
    }

    private fun moveTailForward(target: Segment) {
        tail.compareAndSet(tail.get(), target)
    }

    private fun moveHeadForward(target: Segment) {
        head.compareAndSet(head.get(), target)
    }

    private fun shouldTryToDequeue(): Boolean {
        while (true) {
            val currentEnqIndex = enqIndex.get()
            val currentDeqIndex  = deqIndex.get()
            if (currentEnqIndex == enqIndex.get()) return currentDeqIndex < currentEnqIndex
        }
    }

    override fun enqueue(element: E) {
        while(true) {
            val currentTail = tail.get()
            val index = enqIndex.getAndIncrement()
            val segment = findSegment(currentTail, index / SEGMENT_SIZE)
            moveTailForward(segment)
            if(segment.cells.compareAndSet((index % SEGMENT_SIZE).toInt(), null, element)) return
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun dequeue(): E? {
        while(true) {
            if (!shouldTryToDequeue()) return null
            val currentHead = head.get()
            val index = deqIndex.getAndIncrement()
            val segment = findSegment(currentHead, index / SEGMENT_SIZE)
            moveHeadForward(segment)
            val cellIdx = (index % SEGMENT_SIZE).toInt()
            if (segment.cells.compareAndSet(cellIdx, null, Tombstone)) continue
            return segment.cells.get(cellIdx) as E
        }
    }
}

private class Segment(val id: Long) {
    val next = AtomicReference<Segment?>(null)
    val cells = AtomicReferenceArray<Any?>(SEGMENT_SIZE)
}

// DO NOT CHANGE THIS CONSTANT
private const val SEGMENT_SIZE = 2