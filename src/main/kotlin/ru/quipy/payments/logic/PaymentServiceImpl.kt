package ru.quipy.payments.logic

import org.springframework.beans.factory.DisposableBean
import org.springframework.stereotype.Service
import java.util.UUID

@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>,
) : PaymentService, DisposableBean {
    // Case 1 uses only acc-3. Never send the same payment to every configured account.
    private val account = requireNotNull(paymentAccounts.filter { it.isEnabled() }.minByOrNull { it.price() }) {
        "No enabled payment accounts configured"
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
    }

    override fun destroy() {
        paymentAccounts.forEach { it.close() }
    }
}
