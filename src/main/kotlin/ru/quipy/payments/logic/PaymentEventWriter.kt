package ru.quipy.payments.logic

import ru.quipy.core.EventSourcingService
import ru.quipy.domain.Event
import ru.quipy.payments.api.PaymentAggregate
import java.util.UUID

/** A single-event boundary; the ES library also has an update overload returning a list. */
fun interface PaymentEventWriter {
    fun update(paymentId: UUID, command: (PaymentAggregateState) -> Event<PaymentAggregate>)
}

class EventSourcingPaymentEventWriter(
    private val service: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
) : PaymentEventWriter {
    override fun update(paymentId: UUID, command: (PaymentAggregateState) -> Event<PaymentAggregate>) {
        service.update(paymentId) { state -> command(state) }
    }
}
