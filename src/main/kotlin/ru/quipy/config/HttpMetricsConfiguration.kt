package ru.quipy.config

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.binder.MeterBinder
import io.micrometer.core.instrument.binder.okhttp3.OkHttpMetricsEventListener
import okhttp3.OkHttpClient
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.payments.logic.PaymentAdmissionControl

@Configuration
class HttpMetricsConfiguration {
    @Bean
    fun paymentAdmissionMetrics(admission: PaymentAdmissionControl): MeterBinder = MeterBinder { registry ->
        Gauge.builder("payment.inflight", admission) { it.inFlightPayments().toDouble() }
            .description("Accepted payment slots held while queued or processing")
            .register(registry)
    }

    @Bean
    fun paymentHttpClient(registry: MeterRegistry): OkHttpClient = OkHttpClient.Builder()
        .eventListener(
            OkHttpMetricsEventListener.builder(registry, "http.client.requests")
                .uriMapper { request ->
                    when (request.url.encodedPath) {
                        "/external/accounts" -> "/external/accounts"
                        "/external/process" -> "/external/process"
                        else -> "other"
                    }
                }
                .build()
        )
        .build()
}
