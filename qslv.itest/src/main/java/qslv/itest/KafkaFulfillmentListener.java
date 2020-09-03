package qslv.itest;

import java.util.concurrent.ArrayBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import qslv.common.kafka.ResponseMessage;
import qslv.common.kafka.TraceableMessage;
import qslv.transaction.request.CancelReservationRequest;
import qslv.transaction.request.CommitReservationRequest;
import qslv.transaction.request.TransactionRequest;
import qslv.transaction.response.CancelReservationResponse;
import qslv.transaction.response.CommitReservationResponse;
import qslv.transaction.response.TransactionResponse;

@Component
public class KafkaFulfillmentListener {
	private static final Logger log = LoggerFactory.getLogger(KafkaFulfillmentListener.class);

	@Autowired
	ArrayBlockingQueue<TraceableMessage<ResponseMessage<CancelReservationRequest, CancelReservationResponse>>> cancelExchangeQueue;
	@Autowired
	ArrayBlockingQueue<TraceableMessage<ResponseMessage<CommitReservationRequest, CommitReservationResponse>>> commitExchangeQueue;
	@Autowired
	ArrayBlockingQueue<TraceableMessage<ResponseMessage<TransactionRequest, TransactionResponse>>> transactionExchangeQueue;

	public void drain(ArrayBlockingQueue<?> queue) {
		queue.clear();
	}

	public void drainAll() {
		cancelExchangeQueue.clear();
		commitExchangeQueue.clear();
		transactionExchangeQueue.clear();
	}

	@KafkaListener(containerFactory = "cancelReservationListenerContainerFactory", topics = {
			"cancel.fulfillment.reply.queue" }, groupId = "foo")
	public void cancelListen(
			@Payload TraceableMessage<ResponseMessage<CancelReservationRequest, CancelReservationResponse>> message) {
		log.debug("cancelListen ENTRY");
		try {
			cancelExchangeQueue.put(message);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			log.debug(e.getLocalizedMessage());
		}
		log.debug("cancelListen EXIT");
	}

	@KafkaListener(containerFactory = "commitReservationListenerContainerFactory", topics = {
			"commit.fulfillment.reply.queue" }, groupId = "foo")
	public void commitListen(
			@Payload TraceableMessage<ResponseMessage<CommitReservationRequest, CommitReservationResponse>> message) {
		log.debug("commitListen ENTRY");
		try {
			commitExchangeQueue.put(message);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			log.debug(e.getLocalizedMessage());
		}
		log.debug("onMessage EXIT");
	}

	@KafkaListener(containerFactory = "transactionListenerContainerFactory", topics = {
			"transaction.fulfillment.reply.queue" }, groupId = "foo")
	public void transactionListen(
			@Payload TraceableMessage<ResponseMessage<TransactionRequest, TransactionResponse>> message) {
		log.debug("transactionListen ENTRY");
		try {
			transactionExchangeQueue.put(message);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			log.debug(e.getLocalizedMessage());
		}
		log.debug("onMessage EXIT");
	}
}
