package day1

import java.util.concurrent.*
import java.util.concurrent.atomic.*

open class TreiberStackWithElimination<E> : Stack<E> {
    private val stack = TreiberStack<E>()

    // TODO: Try to optimize concurrent push and pop operations,
    // TODO: synchronizing them in an `eliminationArray` cell.
    private val eliminationArray = AtomicReferenceArray<Any?>(ELIMINATION_ARRAY_SIZE)


    override fun push(element: E) {
        if (tryPushElimination(element)) return
        stack.push(element)
    }

    protected open fun tryPushElimination(element: E): Boolean {
        val cellIndex = randomCellIndex()
        if(!eliminationArray.compareAndSet(cellIndex, CELL_STATE_EMPTY, element)) return false

        repeat(ELIMINATION_WAIT_CYCLES) {
            if(eliminationArray.compareAndSet(cellIndex, CELL_STATE_RETRIEVED, CELL_STATE_EMPTY)) return true
        }
        if(eliminationArray.compareAndSet(cellIndex, element, CELL_STATE_EMPTY)) return false

        eliminationArray.set(cellIndex, CELL_STATE_EMPTY)
        return true
    }

    override fun pop(): E? = tryPopElimination() ?: stack.pop()

    private fun tryPopElimination(): E? {
        val cellIndex = randomCellIndex()
        val currentValue = eliminationArray.get(cellIndex)
        if(currentValue == CELL_STATE_RETRIEVED || currentValue == CELL_STATE_EMPTY) return null
        if(eliminationArray.compareAndSet(cellIndex, currentValue, CELL_STATE_RETRIEVED)) return currentValue as E
        return null
    }

    private fun randomCellIndex(): Int =
        ThreadLocalRandom.current().nextInt(eliminationArray.length())

    companion object {
        private const val ELIMINATION_ARRAY_SIZE = 2 // Do not change!
        private const val ELIMINATION_WAIT_CYCLES = 1 // Do not change!

        // Initially, all cells are in EMPTY state.
        private val CELL_STATE_EMPTY = null

        // `tryPopElimination()` moves the cell state
        // to `RETRIEVED` if the cell contains element.
        private val CELL_STATE_RETRIEVED = Any()
    }
}