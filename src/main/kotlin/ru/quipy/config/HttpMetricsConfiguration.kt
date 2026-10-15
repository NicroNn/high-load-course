package ru.quipy.config

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.okhttp3.OkHttpMetricsEventListener
import okhttp3.OkHttpClient
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class HttpMetricsConfiguration {
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
