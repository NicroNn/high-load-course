package ru.quipy.payments.logic

import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean

@Component
class PaymentAdmissionControl(
    @Value("\${payment.max-in-flight}") private val maxInFlight: Int,
) {
    init {
        require(maxInFlight > 0) { "payment.max-in-flight must be positive" }
    }

    private val slots = Semaphore(maxInFlight)

    fun tryAcquire(): Permit? = if (slots.tryAcquire()) Permit(slots) else null

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
