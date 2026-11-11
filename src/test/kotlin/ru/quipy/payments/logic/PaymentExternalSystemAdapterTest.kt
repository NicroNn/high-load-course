package ru.quipy.payments.logic

import com.sun.net.httpserver.HttpServer
import okhttp3.OkHttpClient
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.mockito.Answers
import org.mockito.Mockito.mock
import org.mockito.stubbing.Answer
import ru.quipy.core.EventSourcingService
import ru.quipy.domain.Event
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.api.PaymentProcessedEvent
import ru.quipy.payments.api.PaymentSubmittedEvent
import java.net.InetSocketAddress
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

class PaymentExternalSystemAdapterTest {
    @Test
    fun `temporary provider error is retried with a new transaction and recorded outcomes`() {
        withPayment(listOf(Outcome(false, "Temporary error"), Outcome(true))) { result ->
            assertEquals(2, result.requests.size)
            assertEquals(listOf(result.paymentId.toString(), result.paymentId.toString()),
                result.requests.map { it.getValue("paymentId") })
            val transactions = result.requests.map { UUID.fromString(it.getValue("transactionId")) }
            assertNotEquals(transactions[0], transactions[1])
            assertEquals(listOf("42", "42"), result.requests.map { it.getValue("amount") })

            assertEquals(listOf(PaymentSubmittedEvent::class, PaymentProcessedEvent::class,
                PaymentSubmittedEvent::class, PaymentProcessedEvent::class), result.events.map { it::class })
            assertEquals(transactions, result.events.filterIsInstance<PaymentSubmittedEvent>().map { it.transactionId })
            val processed = result.events.filterIsInstance<PaymentProcessedEvent>()
            assertEquals(transactions, processed.map { it.transactionId })
            assertEquals(listOf(false, true), processed.map { it.success })
            assertEquals(listOf("Temporary error", null), processed.map { it.reason })
        }
    }

    @Test
    fun `successful payment is submitted once`() {
        withPayment(listOf(Outcome(true))) { result ->
            assertEquals(1, result.requests.size)
            assertEquals(1, result.events.filterIsInstance<PaymentSubmittedEvent>().size)
            assertTrue(result.events.filterIsInstance<PaymentProcessedEvent>().single().success)
        }
    }

    @Test
    fun `permanent provider failure is not retried`() {
        withPayment(listOf(Outcome(false, "Payment declined"))) { result ->
            assertEquals(1, result.requests.size)
            val processed = result.events.filterIsInstance<PaymentProcessedEvent>().single()
            assertFalse(processed.success)
            assertEquals("Payment declined", processed.reason)
        }
    }

    @Test
    fun `temporary error is not retried without enough deadline budget`() {
        withPayment(listOf(Outcome(false, "Temporary error")), budgetMillis = 1_500) { result ->
            assertEquals(1, result.requests.size)
            val processed = result.events.filterIsInstance<PaymentProcessedEvent>().single()
            assertFalse(processed.success)
            assertEquals("Temporary error", processed.reason)
        }
    }

    private data class Outcome(val success: Boolean, val message: String? = null)

    private data class Result(
        val paymentId: UUID,
        val requests: List<Map<String, String>>,
        val events: List<Event<PaymentAggregate>>,
    )

    @Suppress("UNCHECKED_CAST")
    private fun withPayment(outcomes: List<Outcome>, budgetMillis: Long = 3_500, assertions: (Result) -> Unit) {
        val paymentId = UUID.randomUUID()
        val state = PaymentAggregateState().apply {
            paymentCreatedApply(create(paymentId, UUID.randomUUID(), 42))
        }
        val events = mutableListOf<Event<PaymentAggregate>>()
        val paymentService = mock(EventSourcingService::class.java, Answer { invocation ->
            if (invocation.method.name == "update") {
                assertEquals(paymentId, invocation.getArgument<UUID>(0))
                val command = invocation.getArgument<(PaymentAggregateState) -> Event<PaymentAggregate>>(2)
                val event = command(state)
                events.add(event)
                when (event) {
                    is PaymentSubmittedEvent -> state.paymentSubmittedApply(event)
                    is PaymentProcessedEvent -> state.paymentSubmittedApply(event)
                }
                null
            } else {
                Answers.RETURNS_DEFAULTS.answer(invocation)
            }
        }) as EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
        val requests = CopyOnWriteArrayList<Map<String, String>>()
        val responseIndex = AtomicInteger()
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.createContext("/external/process") { exchange ->
            val parameters = exchange.requestURI.rawQuery.split('&').associate {
                val (key, value) = it.split('=', limit = 2)
                key to value
            }
            requests.add(parameters)
            val outcome = outcomes.getOrElse(responseIndex.getAndIncrement()) { Outcome(true) }
            val message = outcome.message?.let { "\"$it\"" } ?: "null"
            val body = """{"transactionId":"${parameters.getValue("transactionId")}","paymentId":"${parameters.getValue("paymentId")}","result":${outcome.success},"message":$message}""".toByteArray()
            exchange.responseHeaders.add("Content-Type", "application/json")
            exchange.sendResponseHeaders(200, body.size.toLong())
            exchange.responseBody.use { it.write(body) }
            exchange.close()
        }
        val client = OkHttpClient()
        server.start()
        try {
            val adapter = PaymentExternalSystemAdapterImpl(
                PaymentAccountProperties("test-service", "acc-8", 50, 10, 30, Duration.ofMillis(700), true),
                paymentService,
                "127.0.0.1:${server.address.port}",
                "test-token",
                client,
                PaymentAdmissionControl(300, 10),
            )
            val startedAt = now()
            adapter.performPaymentAsync(paymentId, 42, startedAt, startedAt + budgetMillis)
            assertions(Result(paymentId, requests.toList(), events))
        } finally {
            server.stop(0)
            client.connectionPool.evictAll()
            client.dispatcher.executorService.shutdownNow()
        }
    }
}
