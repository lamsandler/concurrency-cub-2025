@file:Suppress("DuplicatedCode", "UNCHECKED_CAST")

package day5

import java.util.concurrent.atomic.*

// This implementation never stores `null` values.
class AtomicArrayWithCAS2<E : Any>(size: Int, initialValue: E) {
    private val array = AtomicReferenceArray<Any?>(size)

    init {
        // Fill array with the initial value.
        for (i in 0 until size) {
            array[i] = initialValue
        }
    }

    private enum class Status {
        UNDECIDED,
        SUCCESS,
        FAILURE
    }

    private sealed interface Descriptor {
        val status: AtomicReference<Status>
    }

    private class CAS2Descriptor<E>(
        val index1: Int,
        val expected1: E?,
        val update1: E?,
        val index2: Int,
        val expected2: E?,
        val update2: E?,
    ): Descriptor {
        override val status = AtomicReference(Status.UNDECIDED)
        val dcssDescriptor = DCSSDescriptor(index2, expected2, this, this.status, Status.UNDECIDED)
    }
    private class DCSSDescriptor<E>(
        val index: Int,
        val expected: E?,
        val update: CAS2Descriptor<E>,
        val statusRef: AtomicReference<Status>,
        val expectedStatus: Status
    ): Descriptor {
        override val status = AtomicReference(Status.UNDECIDED)
    }

    fun get(index: Int): E? {
        while(true) {
            when(val element = array.get(index)) {
                is CAS2Descriptor<*> -> {
                    (element as CAS2Descriptor<E>).work()
                }
                is DCSSDescriptor<*> -> {
                    (element as DCSSDescriptor<E>).work()
                }
                else -> return element as E?
            }
        }
    }

    private fun CAS2Descriptor<E>.install(): Boolean {
        while(true) {
            if (status.get() != Status.UNDECIDED) return status.get() == Status.SUCCESS
            when(val element = array.get(index1)) {
                this -> return true
                is CAS2Descriptor<*> -> {
                    (element as CAS2Descriptor<E>).work()
                }
                is DCSSDescriptor<*> -> {
                    (element as DCSSDescriptor<E>).work()
                }
                expected1 -> {
                    if (array.compareAndSet(index1, element, this)) return true
                }
                else -> return false
            }
        }

    }

    private fun DCSSDescriptor<E>.install(): Boolean {
        while (true) {
            if (status.get() != Status.UNDECIDED) return status.get() == Status.SUCCESS
            when (val element = array.get(index)) {
                expected -> {
                    if (array.compareAndSet(index, element, this)) return true
                }
                this -> return true
                is DCSSDescriptor<*> -> {
                    (element as DCSSDescriptor<E>).work()
                }
                is CAS2Descriptor<*> -> {
                    (element as CAS2Descriptor<E>).work()
                }
                else -> return false
            }
        }

    }

    private fun DCSSDescriptor<E>.work() {
        if (status.get() == Status.UNDECIDED) {
            if (!this.install()) {
                status.compareAndSet(Status.UNDECIDED, Status.FAILURE)
            } else {
                status.compareAndSet(Status.UNDECIDED, Status.SUCCESS)
            }
        }
        val newValue = if (statusRef.get() == expectedStatus) update else expected
        array.compareAndSet(index, this, newValue)
    }

    private fun dcss(
        descriptor: DCSSDescriptor<E>,
    ): Boolean {
        descriptor.work()
        return descriptor.status.get() == Status.SUCCESS
    }

    private fun CAS2Descriptor<E>.work() {
        if (status.get() == Status.UNDECIDED) {
            if (!this.install()) {
                status.compareAndSet(Status.UNDECIDED, Status.FAILURE)
            } else {
                if (!dcss(dcssDescriptor)) {
                    status.compareAndSet(Status.UNDECIDED, Status.FAILURE)
                } else {
                    status.compareAndSet(Status.UNDECIDED, Status.SUCCESS)
                }
            }
        }
        val (newValue1, newValue2) = if (status.get() == Status.SUCCESS) {
            update1 to update2
        } else {
            expected1 to expected2
        }
        array.compareAndSet(index1, this, newValue1)
        array.compareAndSet(index2, this, newValue2)
    }

    fun cas2(
        index1: Int, expected1: E?, update1: E?,
        index2: Int, expected2: E?, update2: E?
    ): Boolean {
        require(index1 != index2) { "The indices should be different" }
        if(index2 < index1) return cas2(index2, expected2, update2, index1, expected1, update1)

        val descriptor = CAS2Descriptor(index1, expected1, update1, index2, expected2, update2)
        descriptor.work()

        return descriptor.status.get() == Status.SUCCESS
    }

}