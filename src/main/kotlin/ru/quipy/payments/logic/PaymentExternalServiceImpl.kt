package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.MeterRegistry
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val client: OkHttpClient,
    private val admission: PaymentAdmissionControl,
    registry: MeterRegistry,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    // Normal acc-7 responses take up to about 1.2 s. Slow responses take over 9 s.
    private val attemptTimeoutMillis = 1_500.takeIf { accountName == "acc-7" }
    private val retryBudgetMillis =
        (attemptTimeoutMillis?.toLong() ?: requestAverageProcessingTime.multipliedBy(2).toMillis()) + 250
    private val retries = registry.counter("payment.retries", "account", accountName)
    private val timeouts = registry.counter("payment.attempt.timeouts", "account", accountName)

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))

    private val parallelSemaphore = OngoingWindow(parallelRequests)

    private val paymentClient = client.newBuilder()
        .apply { attemptTimeoutMillis?.let { readTimeout(it.toLong(), TimeUnit.MILLISECONDS) } }
        .addNetworkInterceptor { chain ->
            rateLimiter.tickBlocking()
            val request = chain.request()
            val attempt = checkNotNull(request.tag(Attempt::class.java))
            if (attemptTimeoutMillis != null && now() + retryBudgetMillis >= attempt.deadline) {
                throw IOException("Insufficient time before payment deadline.")
            }
            admission.onDispatch()
            try {
                if (attempt.retry) retries.increment()
                chain.proceed(request)
            } finally {
                admission.onCompletion()
            }
        }
        .build()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        var retry = false
        do {
            if (!performPaymentAttempt(paymentId, amount, paymentStartedAt, Attempt(deadline, retry))) return
            retry = true
        } while (now() + retryBudgetMillis < deadline)
    }

    private data class Attempt(val deadline: Long, val retry: Boolean)

    private fun performPaymentAttempt(paymentId: UUID, amount: Int, paymentStartedAt: Long, attempt: Attempt): Boolean {
        parallelSemaphore.acquire()

        try {
            logger.warn("[$accountName] Submitting payment request for payment $paymentId")

            val transactionId = UUID.randomUUID()

            // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
            // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }

            logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

            try {
                val request = Request.Builder().run {
                    url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                    tag(Attempt::class.java, attempt)
                    post(emptyBody)
                }.build()

                paymentClient.newCall(request).execute().use { response ->
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(),false, e.message)
                    }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                    // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                    // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                    return !body.result && body.message == "Temporary error"
                }
            } catch (e: Exception) {
                when (e) {
                    is SocketTimeoutException -> {
                        timeouts.increment()
                        logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                        }
                        return attemptTimeoutMillis != null
                    }

                    else -> {
                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = e.message)
                        }
                    }
                }
            }
        } finally {
            parallelSemaphore.release()
        }
        return false
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()
