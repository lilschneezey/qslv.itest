package qslv.itest;

import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;


import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import qslv.common.TraceableRequest;
import qslv.common.kafka.TraceableMessage;
import qslv.transaction.resource.TransactionResource;
import qslv.transfer.request.TransferFulfillmentMessage;
import qslv.transfer.request.TransferFundsRequest;
import qslv.transfer.response.TransferFundsResponse;
import qslv.util.EnableQuickSilver;

@SpringBootTest
@EnableQuickSilver
class Itest_TransferandFulfillFunds {
			
	@Autowired
	TransferFundsDao transferFundsDao;
	@Autowired
	JdbcDao jdbcDao;
	@Autowired
	ConfigProperties config;
	@Autowired
	KafkaTransferFundsRequestListener kafkaTransferFundsRequestListener;
	@Autowired
	ArrayBlockingQueue<TransferFulfillmentMessage> transferFundsRequestexchangeQueue;
	@Autowired
	KafkaProducerDao kafkaProducerDao;
	
	//DeadLetter
	@Autowired KafkaTransferFundsDeadLetterListener kafkaTransferFundsDeadLetterListener;
	@Autowired ArrayBlockingQueue<String> deadLetterExchangeQueue;
	
	public static String TEST_FROM_ACCOUNT = "TEST_ACCOUNT";
	public static String TEST_TO_ACCOUNT = "TEST_ACCOUNT1";
	public static String TEST_TAXONOMY_ID = "9.9.9.9.9";
	public static String VALID_STATUS = "EF";
	public static String INVALID_STATUS = "CL";
	public static String JSON_DATA = "{\"value\": 234934}";
	
	@Test
	void testTransferFunds_success() throws Exception {
		long start_from_amount = 9999L;
		long start_to_amount = 4444L;
		long transfer_amount = 8888L;
		long expected_from_balance = start_from_amount - transfer_amount;
		long expected_to_balance = start_to_amount + transfer_amount;
		UUID requestUuid = UUID.randomUUID();
		
		// - setup --------------------
		jdbcDao.setupAccountBalance(TEST_FROM_ACCOUNT, start_from_amount);
		jdbcDao.setupAccount(TEST_FROM_ACCOUNT, VALID_STATUS);
		jdbcDao.setupAccountBalance(TEST_TO_ACCOUNT, start_to_amount);
		jdbcDao.setupAccount(TEST_TO_ACCOUNT, VALID_STATUS);
		
		HashMap<String, String> headerMap = setup_header();
		
		TransferFundsRequest request = new TransferFundsRequest();
		request.setRequestUuid(requestUuid);
		request.setFromAccountNumber(TEST_FROM_ACCOUNT);
		request.setToAccountNumber(TEST_TO_ACCOUNT);
		request.setTransactionAmount(transfer_amount);
		request.setTransactionJsonMetaData(JSON_DATA);
		
		// - execute ------------------
		kafkaTransferFundsRequestListener.setListening(true);
		TransferFundsResponse response = transferFundsDao.transferFunds(headerMap, request);
	
		// - verify -------------------
		assertNotNull(response);
		assertEquals(TransferFundsResponse.SUCCESS, response.getStatus());
		assertNotNull(response.getReservation());
		TransactionResource reservation = response.getReservation();
		TransferFulfillmentMessage message = response.getFulfillmentMessage();

		//-----------------------
		// Kafka Queue Check
		//-----------------------
		TransferFulfillmentMessage kmessage = transferFundsRequestexchangeQueue.take();
		kafkaTransferFundsRequestListener.setListening(false);
		assertEquals(message.getFromAccountNumber(), kmessage.getFromAccountNumber());
		assertEquals(message.getRequestUuid(), kmessage.getRequestUuid());
		assertEquals(message.getReservationUuid(), kmessage.getReservationUuid());
		assertEquals(message.getToAccountNumber(), kmessage.getToAccountNumber());
		assertEquals(message.getTransactionAmount(), kmessage.getTransactionAmount());
		assertEquals(message.getTransactionMetaDataJson(), kmessage.getTransactionMetaDataJson());

		Thread.sleep(2000L);
		
		long fromBalance = jdbcDao.selectBalance(TEST_FROM_ACCOUNT);
		long toBalance = jdbcDao.selectBalance(TEST_TO_ACCOUNT);
		assertEquals( expected_from_balance, fromBalance );
		assertEquals( expected_to_balance, toBalance );
		
		assertNotNull( reservation.getTransactionUuid());
		TransactionResource dbTransaction = jdbcDao.selectTransaction(reservation.getTransactionUuid());
		
		// verify original data passed through
		assertEquals( expected_from_balance, reservation.getRunningBalanceAmount());
		assertEquals( (0L-transfer_amount), reservation.getTransactionAmount());
		assertEquals( TEST_FROM_ACCOUNT, reservation.getAccountNumber());
		assertEquals( JSON_DATA, reservation.getTransactionMetaDataJson());
		assertEquals( requestUuid, reservation.getRequestUuid());
		assertEquals( TransactionResource.RESERVATION, reservation.getTransactionTypeCode());
		assertNull( reservation.getDebitCardNumber());
		assertNull( reservation.getReservationUuid());
		
		//verify database matches returned data
		assertNull(dbTransaction.getReservationUuid());
		assertNull(dbTransaction.getDebitCardNumber());
		assertEquals(dbTransaction.getAccountNumber(), reservation.getAccountNumber());
		assertEquals(dbTransaction.getRequestUuid(), reservation.getRequestUuid());
		assertEquals(dbTransaction.getReservationUuid(), reservation.getReservationUuid());
		assertEquals(dbTransaction.getRunningBalanceAmount(), reservation.getRunningBalanceAmount());
		assertEquals(dbTransaction.getTransactionAmount(), reservation.getTransactionAmount());
		assertEquals(dbTransaction.getTransactionMetaDataJson(), reservation.getTransactionMetaDataJson());
		assertEquals(dbTransaction.getTransactionTypeCode(), reservation.getTransactionTypeCode());
		assertEquals(dbTransaction.getTransactionUuid(), reservation.getTransactionUuid());
		
		//verify message details
		assertNotNull( message );
		assertEquals(reservation.getTransactionUuid(), message.getRequestUuid()); //<-- critical to idempotency
		assertEquals(TEST_FROM_ACCOUNT, message.getFromAccountNumber() );
		assertEquals(reservation.getTransactionUuid(), message.getReservationUuid());
		assertEquals(TEST_TO_ACCOUNT, message.getToAccountNumber());
		assertEquals(transfer_amount, message.getTransactionAmount());
		assertEquals(JSON_DATA, message.getTransactionMetaDataJson());
		
		//-----------------------
		// fulfilled transactions
		//-----------------------
		TransactionResource transfer = jdbcDao.selectTransactionbyRequest(message.getRequestUuid(), TEST_TO_ACCOUNT);
		TransactionResource commit = jdbcDao.selectTransactionbyRequest(message.getRequestUuid(), TEST_FROM_ACCOUNT);

		assertEquals(expected_to_balance, transfer.getRunningBalanceAmount());
		assertEquals(transfer_amount, transfer.getTransactionAmount());
		assertEquals(JSON_DATA, transfer.getTransactionMetaDataJson());
		assertEquals(TransactionResource.NORMAL, transfer.getTransactionTypeCode());
		
		assertEquals(expected_from_balance, commit.getRunningBalanceAmount());
		assertEquals(0L, commit.getTransactionAmount());
		assertEquals(JSON_DATA, commit.getTransactionMetaDataJson());
		assertEquals(TransactionResource.RESERVATION_COMMIT, commit.getTransactionTypeCode());

		//-----------------------
		// Start Idempotency Check
		//-----------------------
		kafkaTransferFundsRequestListener.setListening(true);
		TransferFundsResponse iresponse = transferFundsDao.transferFunds(headerMap, request);
		TransferFulfillmentMessage imessage = transferFundsRequestexchangeQueue.take();
		kafkaTransferFundsRequestListener.setListening(false);

		assertTransactionsEquals(response.getReservation(), iresponse.getReservation());
		assertEquals(message.getFromAccountNumber(), imessage.getFromAccountNumber());
		assertEquals(message.getRequestUuid(), imessage.getRequestUuid());
		assertEquals(message.getReservationUuid(), imessage.getReservationUuid());
		assertEquals(message.getToAccountNumber(), imessage.getToAccountNumber());
		assertEquals(message.getTransactionAmount(), imessage.getTransactionAmount());
		assertEquals(message.getTransactionMetaDataJson(), imessage.getTransactionMetaDataJson());
		assertEquals(message.getVersion(), imessage.getVersion());
		
		long ifromBalance = jdbcDao.selectBalance(TEST_FROM_ACCOUNT);
		long itoBalance = jdbcDao.selectBalance(TEST_TO_ACCOUNT);

		assertEquals(fromBalance, ifromBalance);
		assertEquals(toBalance, itoBalance);		
	}

	
	@Test
	void test_transferFulfillemnt_noCommitReservation() throws InterruptedException {
		long start_to_amount = 1111L;
		long start_from_amount = 9999L;
		long transfer_amount = 8888L;
		long expected_to_balance = start_to_amount + transfer_amount;
		
		// - setup --------------------
		jdbcDao.setupAccountBalance(TEST_TO_ACCOUNT, start_to_amount);
		jdbcDao.setupAccountBalance(TEST_FROM_ACCOUNT, start_from_amount);
		
		TraceableMessage<TransferFulfillmentMessage> message = setup_message();
		message.getPayload().setTransactionAmount(transfer_amount);
		kafkaTransferFundsDeadLetterListener.setListening(true);
		kafkaProducerDao.produceTransferFulfillmentMessage(message);
		String deadLetter = deadLetterExchangeQueue.take();
		kafkaTransferFundsDeadLetterListener.setListening(false);
		
		long toBalance = jdbcDao.selectBalance(TEST_TO_ACCOUNT);
		assertEquals( expected_to_balance, toBalance );
		long fromBalance = jdbcDao.selectBalance(TEST_FROM_ACCOUNT);
		assertEquals( start_from_amount, fromBalance );
		
		assertTrue( deadLetter.contains("HttpClientErrorException.NotFound"));

	}
	
	@Test
	void test_transferFulfillemnt_malformed_fromAccount() throws InterruptedException {
		// - setup --------------------
		TraceableMessage<TransferFulfillmentMessage> message = setup_message();
		message.getPayload().setFromAccountNumber(null);
		
		//execute
		kafkaTransferFundsDeadLetterListener.setListening(true);
		kafkaProducerDao.produceTransferFulfillmentMessage(message);
		String deadLetter = deadLetterExchangeQueue.take();
		kafkaTransferFundsDeadLetterListener.setListening(false);
		
		assertTrue( deadLetter.contains("Missing From Account Number"));
	}
	
	@Test
	void test_transferFulfillemnt_malformed_toAccount() throws InterruptedException {
		// - setup --------------------
		TraceableMessage<TransferFulfillmentMessage> message = setup_message();
		message.getPayload().setToAccountNumber(null);
		
		//execute
		kafkaTransferFundsDeadLetterListener.setListening(true);
		kafkaProducerDao.produceTransferFulfillmentMessage(message);
		String deadLetter = deadLetterExchangeQueue.take();
		kafkaTransferFundsDeadLetterListener.setListening(false);
		
		assertTrue( deadLetter.contains("Missing From To Number"));
	}
	
	@Test
	void test_transferFulfillemnt_malformed_requestUUID() throws InterruptedException {
		// - setup --------------------
		TraceableMessage<TransferFulfillmentMessage> message = setup_message();
		message.getPayload().setRequestUuid(null);
		
		//execute
		kafkaTransferFundsDeadLetterListener.setListening(true);
		kafkaProducerDao.produceTransferFulfillmentMessage(message);
		String deadLetter = deadLetterExchangeQueue.take();
		kafkaTransferFundsDeadLetterListener.setListening(false);
		
		assertTrue( deadLetter.contains("Missing From Request UUID"));
	}
	
	@Test
	void test_transferFulfillemnt_malformed_reservationUUID() throws InterruptedException {
		// - setup --------------------
		TraceableMessage<TransferFulfillmentMessage> message = setup_message();
		message.getPayload().setReservationUuid(null);
		
		//execute
		kafkaTransferFundsDeadLetterListener.setListening(true);
		kafkaProducerDao.produceTransferFulfillmentMessage(message);
		String deadLetter = deadLetterExchangeQueue.take();
		kafkaTransferFundsDeadLetterListener.setListening(false);
		
		assertTrue( deadLetter.contains("Missing From Reservation UUID"));
	}
	
	@Test
	void test_transferFulfillemnt_malformed_amountZerot() throws InterruptedException {
		// - setup --------------------
		TraceableMessage<TransferFulfillmentMessage> message = setup_message();
		message.getPayload().setTransactionAmount(0L);
		
		//execute
		kafkaTransferFundsDeadLetterListener.setListening(true);
		kafkaProducerDao.produceTransferFulfillmentMessage(message);
		String deadLetter = deadLetterExchangeQueue.take();
		kafkaTransferFundsDeadLetterListener.setListening(false);
		
		assertTrue( deadLetter.contains("Amount less than or equal to 0"));
	}
	
	@Test
	void test_transferFulfillemnt_malformed_amountLTzero() throws InterruptedException {
		// - setup --------------------
		TraceableMessage<TransferFulfillmentMessage> message = setup_message();
		message.getPayload().setTransactionAmount(-1L);
		
		//execute
		kafkaTransferFundsDeadLetterListener.setListening(true);
		kafkaProducerDao.produceTransferFulfillmentMessage(message);
		String deadLetter = deadLetterExchangeQueue.take();
		kafkaTransferFundsDeadLetterListener.setListening(false);
		
		assertTrue( deadLetter.contains("Amount less than or equal to 0"));
	}
	
	@Test
	void test_transferFulfillemnt_malformed_jsonMetaData() throws InterruptedException {
		// - setup --------------------
		TraceableMessage<TransferFulfillmentMessage> message = setup_message();
		message.getPayload().setTransactionMetaDataJson(null);
		
		//execute
		kafkaTransferFundsDeadLetterListener.setListening(true);
		kafkaProducerDao.produceTransferFulfillmentMessage(message);
		String deadLetter = deadLetterExchangeQueue.take();
		kafkaTransferFundsDeadLetterListener.setListening(false);
		
		assertTrue( deadLetter.contains("Missing Meta Data"));
	}

	
	@Test
	void test_transferFulfillemnt_malformed_ait() throws InterruptedException {
		// - setup --------------------
		TraceableMessage<TransferFulfillmentMessage> message = setup_message();
		message.setProducerAit(null);
		
		//execute
		kafkaTransferFundsDeadLetterListener.setListening(true);
		kafkaProducerDao.produceTransferFulfillmentMessage(message);
		String deadLetter = deadLetterExchangeQueue.take();
		kafkaTransferFundsDeadLetterListener.setListening(false);
		
		assertTrue( deadLetter.contains("Missing Producer AIT Id"));
	}
	
	@Test
	void test_transferFulfillemnt_malformed_correlation() throws InterruptedException {
		// - setup --------------------
		TraceableMessage<TransferFulfillmentMessage> message = setup_message();
		message.setCorrelationId(null);
		
		//execute
		kafkaTransferFundsDeadLetterListener.setListening(true);
		kafkaProducerDao.produceTransferFulfillmentMessage(message);
		String deadLetter = deadLetterExchangeQueue.take();
		kafkaTransferFundsDeadLetterListener.setListening(false);
		
		assertTrue( deadLetter.contains("Missing Correlation Id"));
	}
	
	@Test
	void test_transferFulfillemnt_malformed_taxonomu() throws InterruptedException {
		// - setup --------------------
		TraceableMessage<TransferFulfillmentMessage> message = setup_message();
		message.setBusinessTaxonomyId(null);
		
		//execute
		kafkaTransferFundsDeadLetterListener.setListening(true);
		kafkaProducerDao.produceTransferFulfillmentMessage(message);
		String deadLetter = deadLetterExchangeQueue.take();
		kafkaTransferFundsDeadLetterListener.setListening(false);
		
		assertTrue( deadLetter.contains("Missing Business Taxonomy Id"));
	}
	
	@Test
	void test_transferFulfillemnt_malformed_creationTime() throws InterruptedException {
		// - setup --------------------
		TraceableMessage<TransferFulfillmentMessage> message = setup_message();
		message.setMessageCreationTime(null);
		
		//execute
		kafkaTransferFundsDeadLetterListener.setListening(true);
		kafkaProducerDao.produceTransferFulfillmentMessage(message);
		String deadLetter = deadLetterExchangeQueue.take();
		kafkaTransferFundsDeadLetterListener.setListening(false);
		
		assertTrue( deadLetter.contains("Missing Message Creation Time"));
	}

	@Test
	void test_transferFulfillemnt_malformed_nullVersion() throws InterruptedException {
		// - setup --------------------
		TraceableMessage<TransferFulfillmentMessage> message = setup_message();
		message.getPayload().setVersion(null);
		
		//execute
		kafkaTransferFundsDeadLetterListener.setListening(true);
		kafkaProducerDao.produceTransferFulfillmentMessage(message);
		String deadLetter = deadLetterExchangeQueue.take();
		kafkaTransferFundsDeadLetterListener.setListening(false);
		
		assertTrue( deadLetter.contains("Invalid version"));
	}
	
	@Test
	void test_transferFulfillemnt_malformed_version() throws InterruptedException {
		// - setup --------------------
		TraceableMessage<TransferFulfillmentMessage> message = setup_message();
		message.getPayload().setVersion("XXX");
		
		//execute
		kafkaTransferFundsDeadLetterListener.setListening(true);
		kafkaProducerDao.produceTransferFulfillmentMessage(message);
		String deadLetter = deadLetterExchangeQueue.take();
		kafkaTransferFundsDeadLetterListener.setListening(false);
		
		assertTrue( deadLetter.contains("Invalid version"));
	}
	
	private TraceableMessage<TransferFulfillmentMessage> setup_message() {
		TransferFulfillmentMessage message  = new TransferFulfillmentMessage();
		message.setFromAccountNumber(TEST_FROM_ACCOUNT);
		message.setRequestUuid(UUID.randomUUID());
		message.setReservationUuid(UUID.randomUUID());
		message.setToAccountNumber(TEST_TO_ACCOUNT);
		message.setTransactionAmount(1L);
		message.setTransactionMetaDataJson(JSON_DATA);
		message.setVersion(TransferFulfillmentMessage.version1_0);
		
		TraceableMessage<TransferFulfillmentMessage> msg = new TraceableMessage<>();
		msg.setPayload(message);	
		msg.setMessageCreationTime(LocalDateTime.now());
		msg.setBusinessTaxonomyId(TEST_TAXONOMY_ID);
		msg.setCorrelationId(UUID.randomUUID().toString());
		msg.setProducerAit(config.getAitid());
		
		return msg;
	}

	private void assertTransactionsEquals(TransactionResource expected, TransactionResource tested) {
		assertEquals(expected.getAccountNumber(), tested.getAccountNumber());
		assertEquals(expected.getDebitCardNumber(), tested.getDebitCardNumber());
		assertNotNull(tested.getInsertTimestamp());
		assertEquals(expected.getRequestUuid(), tested.getRequestUuid());
		assertEquals(expected.getReservationUuid(), tested.getReservationUuid());
		assertEquals(expected.getRunningBalanceAmount(), tested.getRunningBalanceAmount());
		assertEquals(expected.getTransactionAmount(), tested.getTransactionAmount());
		assertEquals(expected.getTransactionMetaDataJson(), tested.getTransactionMetaDataJson());
		assertEquals(expected.getTransactionTypeCode(), tested.getTransactionTypeCode());
		assertEquals(expected.getTransactionUuid(), tested.getTransactionUuid());
	}

	private HashMap<String, String> setup_header() {
		HashMap<String, String> headerMap = new HashMap<>();
		headerMap.put(TraceableRequest.AIT_ID, config.getAitid());
		headerMap.put(TraceableRequest.BUSINESS_TAXONOMY_ID, TEST_TAXONOMY_ID);
		headerMap.put(TraceableRequest.CORRELATION_ID, UUID.randomUUID().toString());
		headerMap.put(TraceableRequest.ACCEPT_VERSION, TransferFundsRequest.Version1_0);
		return headerMap;
	}

}
