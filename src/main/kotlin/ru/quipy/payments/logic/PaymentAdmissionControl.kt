package ru.quipy.payments.logic

import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.util.ArrayDeque
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean

@Component
class PaymentAdmissionControl(
    @Value("\${payment.max-in-flight}") private val maxInFlight: Int,
    @Value("\${payment.max-rps}") private val maxRps: Int,
) {
    init {
        require(maxInFlight > 0) { "payment.max-in-flight must be positive" }
        require(maxRps > 0) { "payment.max-rps must be positive" }
    }

    private val slots = Semaphore(maxInFlight)
    private val starts = ArrayDeque<Long>()
    private var processing = 0

    fun tryAcquire(deadline: Long): Permit? = tryAcquire(deadline, System.currentTimeMillis())

    @Synchronized
    internal fun tryAcquire(deadline: Long, now: Long): Permit? {
        while (starts.isNotEmpty() && starts.first <= now - 1_000) starts.removeFirst()
        // Project queued starts from actual dispatches, so limiter/network delays do not accumulate drift.
        val schedule = starts.toMutableList()
        repeat(inFlightPayments() - processing + 1) {
            schedule.add(if (schedule.size < maxRps) now
                else maxOf(now, schedule[schedule.size - maxRps] + 1_000))
        }
        // acc-23 takes up to ~2 seconds; keep another 250 ms for the database/network.
        if (schedule.last() + 2_250 > deadline || !slots.tryAcquire()) return null
        return Permit(slots)
    }

    @Synchronized
    fun onDispatch(now: Long = System.currentTimeMillis()) {
        while (starts.isNotEmpty() && starts.first <= now - 1_000) starts.removeFirst()
        starts.addLast(now)
        processing++
    }

    @Synchronized
    fun onCompletion() {
        processing--
    }

    fun inFlightPayments(): Int = maxInFlight - slots.availablePermits()

    class Permit internal constructor(private val slots: Semaphore) : AutoCloseable {
        private val released = AtomicBoolean(false)

        override fun close() {
            if (released.compareAndSet(false, true)) {
                slots.release()
            }
        }
    }
}
