package qslv.itest;


import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.web.server.ResponseStatusException;

import qslv.common.kafka.TraceableMessage;
import qslv.transaction.request.CancelReservationRequest;
import qslv.transaction.request.CommitReservationRequest;
import qslv.transaction.request.TransactionRequest;
import qslv.transfer.request.TransferFulfillmentMessage;

@Repository
@DependsOn({"kafkaPropertiesConfig", "kafkaProducerConfig"})
public class KafkaProducerDao {
	private static final Logger log = LoggerFactory.getLogger(KafkaProducerDao.class);

	@Autowired
	private ConfigProperties config;
	
	@Autowired
	private KafkaTemplate<String, TraceableMessage<TransferFulfillmentMessage>> transferFulfillmentKafkaTemplate;
	@Autowired
	private KafkaTemplate<String, TraceableMessage<CancelReservationRequest>> cancelFulfillmentKafkaTemplate;
	@Autowired
	private KafkaTemplate<String, TraceableMessage<CommitReservationRequest>> commitFulfillmentKafkaTemplate;
	@Autowired
	private KafkaTemplate<String, TraceableMessage<TransactionRequest>> transactionFulfillmentKafkaTemplate;
	
	public void produceTransferFulfillmentMessage(TraceableMessage<TransferFulfillmentMessage> tfr) throws ResponseStatusException {
		try {
			// retry handled internally by kafka using retries & retry.backoff.ms in properties file
			ProducerRecord<String ,TraceableMessage<TransferFulfillmentMessage>> record = 
					transferFulfillmentKafkaTemplate.send(config.getKafkaTransferRequestQueue(),
							tfr.getPayload().getFromAccountNumber(), tfr)
				.get().getProducerRecord();
			log.debug("Kakfa Produce {}", record.value().getPayload());
		} catch ( Exception ex) {
			log.debug(ex.getLocalizedMessage());
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Kafka Producer failure", ex);
		}
	}
	
	public void produceCancelMessage(TraceableMessage<CancelReservationRequest> tfr) throws ResponseStatusException {
		try {
			// retry handled internally by kafka using retries & retry.backoff.ms in properties file
			ProducerRecord<String ,TraceableMessage<CancelReservationRequest>> record = 
					cancelFulfillmentKafkaTemplate.send(config.getKafkaCancelRequestQueue(),
							(tfr.getPayload()==null ? "XXXX" : tfr.getPayload().getAccountNumber() ) , tfr)
				.get().getProducerRecord();
			log.debug("Kakfa Produce {}", record.value().getPayload());
		} catch ( Exception ex) {
			log.debug(ex.getLocalizedMessage());
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Kafka Producer failure", ex);
		}
	}
	
	public void produceCommitMessage(TraceableMessage<CommitReservationRequest> tfr) throws ResponseStatusException {
		try {
			// retry handled internally by kafka using retries & retry.backoff.ms in properties file
			ProducerRecord<String ,TraceableMessage<CommitReservationRequest>> record = 
					commitFulfillmentKafkaTemplate.send(config.getKafkaCommitRequestQueue(),
							(tfr.getPayload()==null ? "XXXX" : tfr.getPayload().getAccountNumber() ), tfr)
				.get().getProducerRecord();
			log.debug("Kakfa Produce {}", record.value().getPayload());
		} catch ( Exception ex) {
			log.debug(ex.getLocalizedMessage());
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Kafka Producer failure", ex);
		}
	}
	
	public void produceTransactionMessage(TraceableMessage<TransactionRequest> tfr) throws ResponseStatusException {
		try {
			// retry handled internally by kafka using retries & retry.backoff.ms in properties file
			ProducerRecord<String ,TraceableMessage<TransactionRequest>> record = 
					transactionFulfillmentKafkaTemplate.send(config.getKafkaTransactionRequestQueue(),
							(tfr.getPayload()==null ? "XXXX" : tfr.getPayload().getAccountNumber() ), tfr)
				.get().getProducerRecord();
			log.debug("Kakfa Produce {}", record.value().getPayload());
		} catch ( Exception ex) {
			log.debug(ex.getLocalizedMessage());
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Kafka Producer failure", ex);
		}
	}
}
