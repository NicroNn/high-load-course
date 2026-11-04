package ru.quipy.payments.logic

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.test.util.ReflectionTestUtils
import ru.quipy.apigateway.APIController
import java.util.UUID
import java.util.concurrent.Callable
import java.util.concurrent.Executors

class PaymentAdmissionControlTest {
    @Test
    fun `short deadline limits the backlog and rejected requests do not occupy slots`() {
        val admission = PaymentAdmissionControl(300, 11)
        val permits = List(11) { requireNotNull(admission.tryAcquire(2_500, 0)) }
        repeat(11) { admission.onDispatch(0) }

        repeat(100) { assertNull(admission.tryAcquire(2_500, 0)) }
        assertEquals(11, admission.inFlightPayments())
        admission.onCompletion()
        permits.first().close()
        permits.first().close()
        assertEquals(10, admission.inFlightPayments())
        assertNull(admission.tryAcquire(2_500, 0))
        assertNotNull(admission.tryAcquire(3_501, 1_001))
        assertEquals(11, admission.inFlightPayments())
    }

    @Test
    fun `long deadlines retain bursts instead of imposing an incoming 11 RPS limit`() {
        val admission = PaymentAdmissionControl(300, 11)
        repeat(264) { assertNotNull(admission.tryAcquire(26_000, 0)) }
        repeat(11) { admission.onDispatch(0) }
        repeat(6) { assertNull(admission.tryAcquire(26_000, 0)) }
        repeat(6) { assertNotNull(admission.tryAcquire(27_001, 1_001)) }
        assertEquals(270, admission.inFlightPayments())
    }

    @Test
    fun `concurrent requests cannot overfill the deadline budget`() {
        val admission = PaymentAdmissionControl(300, 11)
        val executor = Executors.newFixedThreadPool(16)
        try {
            val permits = executor.invokeAll(List(500) { Callable { admission.tryAcquire(13_000, 0) } })
                .mapNotNull { it.get() }
            assertEquals(121, permits.size)
            permits.forEach { it.close() }
            assertEquals(0, admission.inFlightPayments())
        } finally {
            executor.shutdownNow()
        }
    }

    @Test
    fun `absolute capacity still applies with a distant deadline`() {
        val admission = PaymentAdmissionControl(5, 11)
        val deadline = System.currentTimeMillis() + 60_000
        repeat(5) { assertNotNull(admission.tryAcquire(deadline)) }
        assertNull(admission.tryAcquire(deadline))
    }

    @Test
    fun `rejection includes the Bombardier retry timestamp before accessing the order`() {
        val admission = PaymentAdmissionControl(300, 11)
        val controller = APIController()
        ReflectionTestUtils.setField(controller, "paymentAdmissionControl", admission)
        val before = System.currentTimeMillis()

        val response = controller.payOrder(UUID.randomUUID(), before)

        assertEquals(429, response.statusCode.value())
        val retryAt = response.headers.getFirst("Retry-After")!!.toLong()
        assertTrue(retryAt >= before + 100)
        assertTrue(retryAt <= System.currentTimeMillis() + 100)
        assertEquals(0, admission.inFlightPayments())
    }
}
