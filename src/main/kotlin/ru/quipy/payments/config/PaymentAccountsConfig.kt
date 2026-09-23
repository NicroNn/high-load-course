package ru.quipy.payments.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.HttpUrl.Companion.toHttpUrl
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.logic.*
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.*


@Configuration
class PaymentAccountsConfig {
    companion object {
        private val javaClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()
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
    fun accountAdapters(paymentService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>): List<PaymentExternalSystemAdapter> {
        require(serviceName.isNotBlank() && token.isNotBlank()) { "Set PAYMENT_SERVICE_NAME and PAYMENT_TOKEN" }
        val requestedAccounts = allowedAccounts.map(String::trim).filter(String::isNotEmpty).toSet()
        require(requestedAccounts.isNotEmpty()) { "Set at least one payment account" }
        val url = "http://$paymentProviderHostPort/external/accounts".toHttpUrl().newBuilder()
            .addQueryParameter("serviceName", serviceName)
            .addQueryParameter("token", token)
            .build()
        val request = HttpRequest.newBuilder()
            .uri(URI(url.toString()))
            .timeout(Duration.ofSeconds(15))
            .GET()
            .build()

        val resp = javaClient.send(request, HttpResponse.BodyHandlers.ofString())
        check(resp.statusCode() == 200) {
            "Cannot load payment accounts (HTTP ${resp.statusCode()}); check credentials and Bombardier"
        }

        println("\nPayment accounts list:")
        val accounts = mapper.readValue<List<PaymentAccountProperties>>(
            resp.body(),
            mapper.typeFactory.constructCollectionType(List::class.java, PaymentAccountProperties::class.java)
        )
            .filter { it.accountName in requestedAccounts }
        val missingAccounts = requestedAccounts - accounts.map { it.accountName }.toSet()
        require(missingAccounts.isEmpty()) { "Provider did not return configured accounts: $missingAccounts" }
        return accounts
            .map { it.copy(enabled = true) }
            .onEach(::println)
            .map {
                PaymentExternalSystemAdapterImpl(
                    it,
                    paymentService,
                    paymentProviderHostPort,
                    token
                )
            }
    }
}
