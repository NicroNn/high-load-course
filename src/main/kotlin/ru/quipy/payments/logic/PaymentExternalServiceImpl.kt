package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.PacedRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {
    init {
        require(properties.parallelRequests > 0) { "parallelRequests must be positive" }
        require(properties.rateLimitPerSec > 0) { "rateLimitPerSec must be positive" }
        require(!properties.averageProcessingTime.isNegative) { "averageProcessingTime must not be negative" }
    }

    private val accountName = properties.accountName
    private val rateLimiter = PacedRateLimiter(properties.rateLimitPerSec)
    private val executor = ThreadPoolExecutor(
        properties.parallelRequests, properties.parallelRequests, 0, TimeUnit.MILLISECONDS,
        ArrayBlockingQueue(8_000), NamedThreadFactory("payment-$accountName"),
        ThreadPoolExecutor.AbortPolicy(),
    )

    // Limit at the network boundary, after connection setup and event-store writes.
    private val client = OkHttpClient.Builder()
        .retryOnConnectionFailure(false)
        .followRedirects(false)
        .followSslRedirects(false)
        .connectTimeout(Duration.ofSeconds(5))
        .readTimeout(Duration.ZERO)
        .addNetworkInterceptor { chain ->
            val timing = requireNotNull(chain.request().tag(PaymentTiming::class.java))
            if (!rateLimiter.acquire(timing.latestStartMillis)) {
                throw IOException("Payment deadline reached before dispatch")
            }
            chain.proceed(chain.request())
        }
        .build()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        try {
            executor.execute {
                try {
                    processPayment(paymentId, amount, paymentStartedAt, deadline)
                } catch (e: Exception) {
                    // Never turn a failed success-event write into a second payment.
                    logger.error("[$accountName] Cannot persist payment outcome for $paymentId (${e.javaClass.simpleName})")
                }
            }
        } catch (e: RejectedExecutionException) {
            recordRejection(paymentId, paymentStartedAt, "Payment queue is full or shutting down")
        }
    }

    private fun processPayment(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        // Average latency is only an estimate. Leave time for recording the result.
        val latestStart = deadline - properties.averageProcessingTime.toMillis() - 250
        if (now() >= latestStart) {
            recordRejection(paymentId, paymentStartedAt, "Insufficient time left to process payment")
            return
        }

        val transactionId = UUID.randomUUID()
        paymentESService.update(paymentId) {
            it.logSubmission(true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val result = try {
            val url = "http://$paymentProviderHostPort/external/process".toHttpUrl().newBuilder()
                .addQueryParameter("serviceName", properties.serviceName)
                .addQueryParameter("token", token)
                .addQueryParameter("accountName", accountName)
                .addQueryParameter("transactionId", transactionId.toString())
                .addQueryParameter("paymentId", paymentId.toString())
                .addQueryParameter("amount", amount.toString())
                .build()
            val request = Request.Builder().url(url)
                .tag(PaymentTiming::class.java, PaymentTiming(latestStart))
                .post(ByteArray(0).toRequestBody()).build()
            val remainingMillis = deadline - now()
            if (remainingMillis <= 0) throw IOException("Payment deadline reached")
            val call = client.newCall(request)
            call.timeout().timeout(remainingMillis, TimeUnit.MILLISECONDS)
            call.execute().use { response ->
                if (!response.isSuccessful) {
                    Result(false, "Provider HTTP ${response.code}")
                } else {
                    val body = mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    if (body.transactionId != transactionId.toString() || body.paymentId != paymentId.toString()) {
                        Result(false, "Provider returned mismatched payment identifiers")
                    } else {
                        Result(body.result, body.message)
                    }
                }
            }
        } catch (e: Exception) {
            if (e is InterruptedException) Thread.currentThread().interrupt()
            // Exception messages can contain the URL (and token). Do not log them.
            Result(false, "Provider request failed: ${e.javaClass.simpleName}; outcome may be unknown")
        }

        paymentESService.update(paymentId) {
            it.logProcessing(result.success, now(), transactionId, result.reason)
        }
        logger.debug("[$accountName] Payment $paymentId completed: ${result.success}")
    }

    private fun recordRejection(paymentId: UUID, paymentStartedAt: Long, reason: String) {
        val transactionId = UUID.randomUUID()
        paymentESService.update(paymentId) {
            it.logSubmission(false, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }
        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), transactionId, reason)
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = accountName

    override fun close() {
        executor.shutdown()
    }

    private data class PaymentTiming(val latestStartMillis: Long)
    private data class Result(val success: Boolean, val reason: String?)

    companion object {
        private val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapterImpl::class.java)
        private val mapper = ObjectMapper().registerKotlinModule()
    }
}

fun now() = System.currentTimeMillis()
