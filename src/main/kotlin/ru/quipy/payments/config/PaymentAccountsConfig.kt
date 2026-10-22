package ru.quipy.payments.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.logic.*
import java.util.*


@Configuration
class PaymentAccountsConfig {
    companion object {
        private val mapper = ObjectMapper().registerKotlinModule().registerModules(JavaTimeModule())
    }

    @Value("\${payment.hostPort}")
    lateinit var paymentProviderHostPort: String

    @Value("\${payment.service-name}")
    lateinit var serviceName: String

    @Value("\${payment.token}")
    lateinit var token: String

    @Value("#{'\${payment.accounts}'.split(',')}")
    lateinit var allowedAccounts: List<String>

    @Bean
    fun accountAdapters(
        paymentService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
        paymentHttpClient: OkHttpClient,
    ): List<PaymentExternalSystemAdapter> {
        val request = Request.Builder()
            .url("http://${paymentProviderHostPort}/external/accounts?serviceName=$serviceName&token=$token")
            .get()
            .build()

        val accounts = paymentHttpClient.newCall(request).execute().use { response ->
            check(response.isSuccessful) { "Payment accounts request failed: HTTP ${response.code}" }
            mapper.readValue<List<PaymentAccountProperties>>(
                checkNotNull(response.body).string(),
                mapper.typeFactory.constructCollectionType(List::class.java, PaymentAccountProperties::class.java)
            )
        }

        println("\nPayment accounts list:")
        return accounts
            .filter { it.accountName in allowedAccounts }
            .map { it.copy(enabled = true) }
            .onEach(::println)
            .map {
                PaymentExternalSystemAdapterImpl(
                    it,
                    paymentService,
                    paymentProviderHostPort,
                    token,
                    paymentHttpClient,
                )
            }
    }
}
