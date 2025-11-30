@file:Suppress("FoldInitializerAndIfToElvis", "DuplicatedCode")

package day4

import java.util.concurrent.atomic.*

class MSQueueWithLinearTimeRemove<E> : QueueWithRemove<E> {
    private val head: AtomicReference<Node>
    private val tail: AtomicReference<Node>

    init {
        val dummy = Node(null)
        head = AtomicReference(dummy)
        tail = AtomicReference(dummy)
    }

    override fun enqueue(element: E) {
        while(true) {
            val currentTail = tail.get()
            val newNode = Node(element)
            if (currentTail.next.compareAndSet(null, newNode)) {
                tail.compareAndSet(currentTail, newNode)
                if (currentTail.extractedOrRemoved) {
                    currentTail.remove()
                }
                return
            } else {
                tail.compareAndSet(currentTail, currentTail.next.get())
            }
        }
    }

    override fun dequeue(): E? {
        while(true) {
            val currentHead = head.get()
            val currentHeadNext = currentHead.next.get() ?: return null
            if (head.compareAndSet(currentHead, currentHeadNext)) {
                if (!currentHeadNext.markExtractedOrRemoved()) continue
                return currentHeadNext.element
            }
        }
    }

    override fun remove(element: E): Boolean {
        // Traverse the linked list, searching the specified
        // element. Try to remove the corresponding node if found.
        // DO NOT CHANGE THIS CODE.
        var node = head.get()
        while (true) {
            val next = node.next.get()
            if (next == null) return false
            node = next
            if (node.element == element && node.remove()) return true
        }
    }

    /**
     * This is an internal function for tests.
     * DO NOT CHANGE THIS CODE.
     */
    override fun validate() {
        check(tail.get().next.get() == null) {
            "tail.next must be null"
        }
        var node = head.get()
        // Traverse the linked list
        while (true) {
            if (node !== head.get() && node !== tail.get()) {
                check(!node.extractedOrRemoved) {
                    "Removed node with element ${node.element} found in the middle of this queue"
                }
            }
            node = node.next.get() ?: break
        }
    }

    private inner class Node(
        var element: E?
    ) {
        val next = AtomicReference<Node?>(null)

        private val _extractedOrRemoved = AtomicBoolean(false)
        val extractedOrRemoved
            get() =
                _extractedOrRemoved.get()

        fun markExtractedOrRemoved(): Boolean =
            _extractedOrRemoved.compareAndSet(false, true)

        private fun findPrevious(): Node? {
            var current = head.get()
            while(true) {
                val next = current.next.get() ?: return null
                if (next === this) return current
                current = next
            }
        }

        /**
         * Removes this node from the queue structure.
         * Returns `true` if this node was successfully
         * removed, or `false` if it has already been
         * removed by [remove] or extracted by [dequeue].
         */
        fun remove(): Boolean {
            val isRemoved = markExtractedOrRemoved()
            val currentPrevious = findPrevious()
            val currentNext = next.get() ?: return isRemoved
            currentPrevious?.next?.set(currentNext)
            if (currentNext.extractedOrRemoved) currentNext.remove()
            return isRemoved
        }
    }
}
