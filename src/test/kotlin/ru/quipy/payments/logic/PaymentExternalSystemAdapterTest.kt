package ru.quipy.payments.logic

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import ru.quipy.core.EventSourcingService
import ru.quipy.domain.Event
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.api.PaymentProcessedEvent
import ru.quipy.payments.api.PaymentSubmittedEvent
import java.net.InetSocketAddress
import java.net.URLDecoder
import java.nio.charset.StandardCharsets.UTF_8
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class PaymentExternalSystemAdapterTest {
    @Test
    fun `burst is queued and HTTP traffic respects rate and parallel limits`() {
        val active = AtomicInteger()
        val maxActive = AtomicInteger()
        val starts = CopyOnWriteArrayList<Long>()
        Provider { exchange ->
            starts.add(System.nanoTime())
            maxActive.accumulateAndGet(active.incrementAndGet(), ::maxOf)
            try {
                Thread.sleep(350)
                exchange.succeed()
            } finally {
                active.decrementAndGet()
            }
        }.use { provider ->
            val events = PaymentEvents()
            adapter(provider, events, parallel = 2, rate = 5).use { adapter ->
                repeat(8) { adapter.submit(events) }
                val results = events.results(8)
                assertTrue(results.all { it.success })
                assertEquals(8, starts.size)
                assertEquals(2, maxActive.get())
                starts.sorted().windowed(6).forEach {
                    // Observe at the server, allowing for small OS/network scheduling jitter.
                    assertTrue(it.last() - it.first() >= TimeUnit.MILLISECONDS.toNanos(980))
                }
                assertEquals(8, events.submissions.size)
                assertEquals(8, results.map { it.transactionId }.toSet().size)
            }
        }
    }

    @Test
    fun `expired queued payment never reaches the provider`() {
        val received = AtomicInteger()
        val firstStarted = CountDownLatch(1)
        val release = CountDownLatch(1)
        Provider { exchange ->
            received.incrementAndGet()
            firstStarted.countDown()
            release.await(5, TimeUnit.SECONDS)
            exchange.succeed()
        }.use { provider ->
            val events = PaymentEvents()
            adapter(provider, events, parallel = 1, rate = 100).use { adapter ->
                adapter.submit(events)
                assertTrue(firstStarted.await(5, TimeUnit.SECONDS))
                adapter.submit(events, deadline = now() + 300)
                Thread.sleep(350)
                release.countDown()
                val results = events.results(2)
                assertEquals(1, results.count { it.success })
                assertEquals(1, received.get())
                assertEquals(1, events.submissions.count { !it.success })
            }
        }
    }

    @Test
    fun `provider HTTP errors and malformed bodies release the worker without retrying`() {
        val received = AtomicInteger()
        Provider { exchange ->
            when (received.incrementAndGet()) {
                1 -> exchange.respond(503, "unavailable")
                2 -> exchange.respond(200, "not-json")
                else -> exchange.succeed()
            }
        }.use { provider ->
            val events = PaymentEvents()
            adapter(provider, events, parallel = 1, rate = 100).use { adapter ->
                repeat(3) { adapter.submit(events) }
                val results = events.results(3)
                assertEquals(listOf(false, false, true), results.map { it.success })
                assertEquals(3, received.get())
                assertEquals(3, events.submissions.size)
            }
        }
    }

    @Test
    fun `success for a different transaction is not accepted`() {
        Provider { it.respond(200, """{"transactionId":"other","paymentId":"other","result":true}""") }
            .use { provider ->
                val events = PaymentEvents()
                adapter(provider, events).use { adapter ->
                    adapter.submit(events)
                    assertFalse(events.results(1).single().success)
                }
            }
    }

    private fun adapter(provider: Provider, events: PaymentEvents, parallel: Int = 1, rate: Int = 10) =
        PaymentExternalSystemAdapterImpl(
            PaymentAccountProperties("test", "acc-3", parallel, rate, 1, Duration.ZERO, true),
            events.service, provider.address, "test-token",
        )

    private fun PaymentExternalSystemAdapter.submit(events: PaymentEvents, deadline: Long = now() + 10_000) {
        val id = UUID.randomUUID()
        events.states[id] = PaymentAggregateState().apply { paymentCreatedApply(create(id, UUID.randomUUID(), 100)) }
        performPaymentAsync(id, 100, now(), deadline)
    }

    private class PaymentEvents {
        val states = ConcurrentHashMap<UUID, PaymentAggregateState>()
        val submissions = CopyOnWriteArrayList<PaymentSubmittedEvent>()
        private val processed = LinkedBlockingQueue<PaymentProcessedEvent>()

        @Suppress("UNCHECKED_CAST")
        val service = Mockito.mock(EventSourcingService::class.java) { invocation ->
            check(invocation.method.name == "update")
            val state = states.getValue(invocation.getArgument(0))
            val command = invocation.getArgument<(PaymentAggregateState) -> Event<PaymentAggregate>>(2)
            synchronized(state) {
                val event = command(state)
                when (event) {
                    is PaymentSubmittedEvent -> {
                        state.paymentSubmittedApply(event)
                        submissions.add(event)
                    }
                    is PaymentProcessedEvent -> {
                        state.paymentSubmittedApply(event)
                        processed.add(event)
                    }
                }
                event
            }
        } as EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

        fun results(count: Int) = (1..count).map {
            requireNotNull(processed.poll(10, TimeUnit.SECONDS)) { "Payment did not complete" }
        }
    }

    private class Provider(handler: (HttpExchange) -> Unit) : AutoCloseable {
        private val executor = Executors.newCachedThreadPool()
        private val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
            this.executor = this@Provider.executor
            createContext("/external/process") { exchange -> exchange.use { handler(it) } }
            start()
        }
        val address get() = "127.0.0.1:${server.address.port}"
        override fun close() {
            server.stop(0)
            executor.shutdownNow()
        }
    }

    companion object {
        private fun HttpExchange.succeed() {
            val params = requestURI.rawQuery.split('&').associate {
                val parts = it.split('=', limit = 2)
                parts[0] to URLDecoder.decode(parts[1], UTF_8)
            }
            respond(200, """{"transactionId":"${params["transactionId"]}","paymentId":"${params["paymentId"]}","result":true}""")
        }

        private fun HttpExchange.respond(status: Int, body: String) {
            val bytes = body.toByteArray(UTF_8)
            sendResponseHeaders(status, bytes.size.toLong())
            responseBody.use { it.write(bytes) }
        }
    }
}
