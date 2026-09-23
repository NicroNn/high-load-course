package ru.quipy.payments.logic

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.UUID

class PaymentSystemTest {
    @Test
    fun `one payment goes to exactly one enabled account`() {
        val disabled = Account(1, false)
        val cheap = Account(2, true)
        val expensive = Account(3, true)
        PaymentSystemImpl(listOf(disabled, expensive, cheap))
            .submitPaymentRequest(UUID.randomUUID(), 100, now(), now() + 80_000)
        assertEquals(listOf(0, 1, 0), listOf(disabled.calls, cheap.calls, expensive.calls))
    }

    @Test
    fun `missing usable account fails at startup`() {
        assertThrows(IllegalArgumentException::class.java) { PaymentSystemImpl(emptyList()) }
        assertThrows(IllegalArgumentException::class.java) { PaymentSystemImpl(listOf(Account(1, false))) }
    }

    private class Account(private val price: Int, private val enabled: Boolean) : PaymentExternalSystemAdapter {
        var calls = 0
        override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) { calls++ }
        override fun name() = "test"
        override fun price() = price
        override fun isEnabled() = enabled
    }
}
