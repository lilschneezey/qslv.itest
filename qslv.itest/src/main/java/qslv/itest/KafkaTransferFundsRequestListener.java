package qslv.itest;

import java.util.concurrent.ArrayBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import qslv.common.kafka.TraceableMessage;
import qslv.transfer.request.TransferFulfillmentMessage;

@Component
public class KafkaTransferFundsRequestListener {
	private static final Logger log = LoggerFactory.getLogger(KafkaTransferFundsRequestListener.class);

	private boolean listening = false;
	
	public void setListening(boolean listening) {
		this.listening = listening;
	}

	@Autowired
	ArrayBlockingQueue<TransferFulfillmentMessage> transferFundsRequestexchangeQueue;
	
	@KafkaListener(topics = { "online.transfer.requests" }, groupId="foo")
	public void listen(@Payload TraceableMessage<TransferFulfillmentMessage> message) {
		if (listening) {
			log.debug("onMessage ENTRY");
			//@SuppressWarnings("unchecked")
			//TraceableMessage<TransferFulfillmentMessage> message = (TraceableMessage<TransferFulfillmentMessage>)data.value();
			try {
				transferFundsRequestexchangeQueue.put(message.getPayload());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				log.debug(e.getLocalizedMessage());
			}
			log.debug("{} From {} To {} Amount {} Reservation {}", 
					message.getPayload().getRequestUuid(),  
					message.getPayload().getFromAccountNumber(),
					message.getPayload().getToAccountNumber(),
					message.getPayload().getTransactionAmount(),
					message.getPayload().getReservationUuid()
			);
			log.debug("onMessage EXIT");
		}
	}
}
