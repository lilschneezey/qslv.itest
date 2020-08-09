package qslv.itest;

import java.util.concurrent.ArrayBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class KafkaTransferFundsDeadLetterListener {
	private static final Logger log = LoggerFactory.getLogger(KafkaTransferFundsDeadLetterListener.class);

	private boolean listening = false;
	
	public void setListening(boolean listening) {
		this.listening = listening;
	}

	@Autowired
	ArrayBlockingQueue<String> deadLetterExchangeQueue;
	
	@KafkaListener(containerFactory = "deadLetterListenerContainerFactory", topics = { "dlq.transfer.requests" }, groupId="foo")
	public void listen(@Payload String message) {
		if (listening) {
			log.debug("onMessage ENTRY");
			//@SuppressWarnings("unchecked")
			//TraceableMessage<TransferFulfillmentMessage> message = (TraceableMessage<TransferFulfillmentMessage>)data.value();
			log.debug(message);
			try {
				deadLetterExchangeQueue.put(message);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				log.debug(e.getLocalizedMessage());
			}
			log.debug("onMessage EXIT");
		}
	}
}
