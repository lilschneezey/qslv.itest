package qslv.itest;

import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;


import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import qslv.common.kafka.ResponseMessage;
import qslv.common.kafka.TraceableMessage;
import qslv.transaction.request.TransactionRequest;
import qslv.transaction.resource.TransactionResource;
import qslv.transaction.response.TransactionResponse;
import qslv.util.EnableQuickSilver;

@SpringBootTest
@EnableQuickSilver
class Itest_TransactionFulfillment {
			
	@Autowired
	TransactionDao transactionDao;
	@Autowired
	JdbcDao jdbcDao;
	@Autowired
	ConfigProperties config;
	@Autowired
	KafkaProducerDao kafkaProducerDao;
	
	//Response Queue
	@Autowired KafkaFulfillmentListener kafkaFulfillmentListener;
	@Autowired ArrayBlockingQueue<TraceableMessage<ResponseMessage<TransactionRequest,TransactionResponse>>> transactionExchangeQueue;
	
	public static String TEST_ACCOUNT = "TEST_ACCOUNT";
	public static String TEST_TAXONOMY_ID = "9.9.9.9.9";
	public static String VALID_STATUS = "EF";
	public static String INVALID_STATUS = "CL";
	public static String JSON_DATA = "{\"value\": 234934}";
	
	@Test
	void testCancelFulfillment_success() throws Exception {
		transactionExchangeQueue.clear();
		
		long start_from_amount = 9999L;
		long transaction_amount = -8888L;
		long expected_balance = start_from_amount + transaction_amount;
		
		// - setup --------------------
		jdbcDao.setupAccountBalance(TEST_ACCOUNT, start_from_amount);
		jdbcDao.setupAccount(TEST_ACCOUNT, VALID_STATUS);
		
		// - setup  -------------------
		UUID transactionUUID = UUID.randomUUID();
		TransactionRequest request = new TransactionRequest();
		request.setAccountNumber(TEST_ACCOUNT);
		request.setRequestUuid(transactionUUID);
		request.setTransactionMetaDataJson(JSON_DATA);
		request.setTransactionAmount(transaction_amount);

		TraceableMessage<TransactionRequest> traceable = new TraceableMessage<>();
		traceable.setProducerAit(config.getAitid());
		traceable.setBusinessTaxonomyId(TEST_TAXONOMY_ID);
		traceable.setCorrelationId("sdfsdfsdf");
		traceable.setPayload(request);
		traceable.setMessageCreationTime(LocalDateTime.now());

		// - Execute
		kafkaProducerDao.produceTransactionMessage(traceable);
		TraceableMessage<ResponseMessage<TransactionRequest,TransactionResponse>>
			response = transactionExchangeQueue.take();

		// - Verify
		assertNotNull( response );
		assertNotNull( response.getPayload());
		assertNotNull( response.getPayload().getRequest() );
		assertEquals( ResponseMessage.SUCCESS, response.getPayload().getStatus() );
		assertNotNull( response.getPayload().getResponse() );
		
		assertEquals( traceable.getBusinessTaxonomyId(), response.getBusinessTaxonomyId() );
		assertEquals( traceable.getCorrelationId(), response.getCorrelationId() );
		assertEquals( traceable.getProducerAit(), response.getProducerAit() );
		assertEquals( traceable.getMessageCreationTime(), response.getMessageCreationTime());
		assertNotNull( response.getMessageCompletionTime() );

		TransactionRequest rrequest = response.getPayload().getRequest();
		TransactionResponse rresponse = response.getPayload().getResponse();

		assertEquals( TEST_ACCOUNT, rrequest.getAccountNumber() );
		assertEquals( transactionUUID, rrequest.getRequestUuid() );
		assertEquals( JSON_DATA, rrequest.getTransactionMetaDataJson() );

		assertEquals( 1, rresponse.getTransactions().size() );
		assertEquals( TransactionResponse.SUCCESS, rresponse.getStatus() );
		assertEquals( TEST_ACCOUNT, rresponse.getTransactions().get(0).getAccountNumber() );
		assertNull( rresponse.getTransactions().get(0).getDebitCardNumber() );
		assertEquals( transactionUUID, rresponse.getTransactions().get(0).getRequestUuid() );
		assertNull( rresponse.getTransactions().get(0).getReservationUuid() );
		assertEquals( expected_balance, rresponse.getTransactions().get(0).getRunningBalanceAmount() );
		assertEquals( transaction_amount, rresponse.getTransactions().get(0).getTransactionAmount() );
		assertEquals( JSON_DATA, rresponse.getTransactions().get(0).getTransactionMetaDataJson() );
		assertEquals( TransactionResource.NORMAL, rresponse.getTransactions().get(0).getTransactionTypeCode() );
		assertNotNull( rresponse.getTransactions().get(0).getTransactionUuid() );

		long dbbalance = jdbcDao.selectBalance(TEST_ACCOUNT);
		assertEquals( dbbalance, expected_balance );
	}
	
	
	@Test
	void test_transferFulfillemnt_malformed_requestUUID() throws InterruptedException {
		transactionExchangeQueue.clear();
		
		// - setup  -------------------
		TransactionRequest request = new TransactionRequest();
		request.setAccountNumber(TEST_ACCOUNT);
		request.setRequestUuid(null);
		request.setTransactionMetaDataJson(JSON_DATA);
		request.setTransactionAmount(1L);

		TraceableMessage<TransactionRequest> traceable = new TraceableMessage<>();
		traceable.setProducerAit(config.getAitid());
		traceable.setBusinessTaxonomyId(TEST_TAXONOMY_ID);
		traceable.setCorrelationId("sdfsdfsdf");
		traceable.setPayload(request);
		traceable.setMessageCreationTime(LocalDateTime.now());
		
		// - Execute
		kafkaProducerDao.produceTransactionMessage(traceable);
		TraceableMessage<ResponseMessage<TransactionRequest,TransactionResponse>>
			response = transactionExchangeQueue.take();

		assertEquals( ResponseMessage.MALFORMED_MESSAGE, response.getPayload().getStatus() );
		assertTrue( response.getPayload().getErrorMessage().contains("Missing From Request UUID"));
	}
	
	@Test
	void test_transferFulfillemnt_malformed_jsonMeta() throws InterruptedException {
		transactionExchangeQueue.clear();
		
		// - setup  -------------------
		UUID transactionUUID = UUID.randomUUID();
		TransactionRequest request = new TransactionRequest();
		request.setAccountNumber(TEST_ACCOUNT);
		request.setRequestUuid(transactionUUID);
		request.setTransactionMetaDataJson(null);
		request.setTransactionAmount(1L);

		TraceableMessage<TransactionRequest> traceable = new TraceableMessage<>();
		traceable.setProducerAit(config.getAitid());
		traceable.setBusinessTaxonomyId(TEST_TAXONOMY_ID);
		traceable.setCorrelationId("sdfsdfsdf");
		traceable.setPayload(request);
		traceable.setMessageCreationTime(LocalDateTime.now());
		
		// - Execute
		kafkaProducerDao.produceTransactionMessage(traceable);
		TraceableMessage<ResponseMessage<TransactionRequest,TransactionResponse>>
			response = transactionExchangeQueue.take();

		assertEquals( ResponseMessage.MALFORMED_MESSAGE, response.getPayload().getStatus() );
		assertTrue( response.getPayload().getErrorMessage().contains("Missing Meta Data"));
	}
	
	@Test
	void test_transferFulfillemnt_malformed_amountZero() throws InterruptedException {
		transactionExchangeQueue.clear();
		
		// - setup  -------------------
		UUID transactionUUID = UUID.randomUUID();
		TransactionRequest request = new TransactionRequest();
		request.setAccountNumber(TEST_ACCOUNT);
		request.setRequestUuid(transactionUUID);
		request.setTransactionMetaDataJson(JSON_DATA);
		request.setTransactionAmount(0L);

		TraceableMessage<TransactionRequest> traceable = new TraceableMessage<>();
		traceable.setProducerAit(config.getAitid());
		traceable.setBusinessTaxonomyId(TEST_TAXONOMY_ID);
		traceable.setCorrelationId("sdfsdfsdf");
		traceable.setPayload(request);
		traceable.setMessageCreationTime(LocalDateTime.now());
		
		// - Execute
		kafkaProducerDao.produceTransactionMessage(traceable);
		TraceableMessage<ResponseMessage<TransactionRequest,TransactionResponse>>
			response = transactionExchangeQueue.take();

		assertEquals( ResponseMessage.MALFORMED_MESSAGE, response.getPayload().getStatus() );
		assertTrue( response.getPayload().getErrorMessage().contains("Transaction Amount must not be zero"));
	}

	@Test
	void test_transferFulfillemnt_malformed_missing_payload() throws InterruptedException {
		transactionExchangeQueue.clear();
		
		// - setup  -------------------
		UUID transactionUUID = UUID.randomUUID();
		TransactionRequest request = new TransactionRequest();
		request.setAccountNumber(TEST_ACCOUNT);
		request.setRequestUuid(transactionUUID);
		request.setTransactionMetaDataJson(JSON_DATA);
		request.setTransactionAmount(1L);

		TraceableMessage<TransactionRequest> traceable = new TraceableMessage<>();
		traceable.setProducerAit(config.getAitid());
		traceable.setBusinessTaxonomyId(TEST_TAXONOMY_ID);
		traceable.setCorrelationId("sdfsdfsdf");
		traceable.setPayload(null);
		traceable.setMessageCreationTime(LocalDateTime.now());
		
		// - Execute
		kafkaProducerDao.produceTransactionMessage(traceable);
		TraceableMessage<ResponseMessage<TransactionRequest,TransactionResponse>>
			response = transactionExchangeQueue.take();

		assertEquals( ResponseMessage.MALFORMED_MESSAGE, response.getPayload().getStatus() );
		assertTrue( response.getPayload().getErrorMessage().contains("Missing Fulfillment Message"));
	}
	
	@Test
	void test_transferFulfillemnt_malformed_missing_ait() throws InterruptedException {
		transactionExchangeQueue.clear();
		
		// - setup  -------------------
		UUID transactionUUID = UUID.randomUUID();
		TransactionRequest request = new TransactionRequest();
		request.setAccountNumber(TEST_ACCOUNT);
		request.setRequestUuid(transactionUUID);
		request.setTransactionMetaDataJson(JSON_DATA);
		request.setTransactionAmount(1L);

		TraceableMessage<TransactionRequest> traceable = new TraceableMessage<>();
		traceable.setProducerAit(null);
		traceable.setBusinessTaxonomyId(TEST_TAXONOMY_ID);
		traceable.setCorrelationId("sdfsdfsdf");
		traceable.setPayload(request);
		traceable.setMessageCreationTime(LocalDateTime.now());
		
		// - Execute
		kafkaProducerDao.produceTransactionMessage(traceable);
		TraceableMessage<ResponseMessage<TransactionRequest,TransactionResponse>>
			response = transactionExchangeQueue.take();

		assertEquals( ResponseMessage.MALFORMED_MESSAGE, response.getPayload().getStatus() );
		assertTrue( response.getPayload().getErrorMessage().contains("Missing Producer AIT Id"));
	}
	
	@Test
	void test_transferFulfillemnt_malformed_missing_correlation() throws InterruptedException {
		transactionExchangeQueue.clear();
		
		// - setup  -------------------
		UUID transactionUUID = UUID.randomUUID();
		TransactionRequest request = new TransactionRequest();
		request.setAccountNumber(TEST_ACCOUNT);
		request.setRequestUuid(transactionUUID);
		request.setTransactionMetaDataJson(JSON_DATA);
		request.setTransactionAmount(1L);

		TraceableMessage<TransactionRequest> traceable = new TraceableMessage<>();
		traceable.setProducerAit(config.getAitid());
		traceable.setBusinessTaxonomyId(TEST_TAXONOMY_ID);
		traceable.setCorrelationId(null);
		traceable.setPayload(request);
		traceable.setMessageCreationTime(LocalDateTime.now());
		
		// - Execute
		kafkaProducerDao.produceTransactionMessage(traceable);
		TraceableMessage<ResponseMessage<TransactionRequest,TransactionResponse>>
			response = transactionExchangeQueue.take();

		assertEquals( ResponseMessage.MALFORMED_MESSAGE, response.getPayload().getStatus() );
		assertTrue( response.getPayload().getErrorMessage().contains("Missing Correlation Id"));
	}
	
	@Test
	void test_transferFulfillemnt_malformed_missing_taxonomy() throws InterruptedException {
		transactionExchangeQueue.clear();
		
		// - setup  -------------------
		UUID transactionUUID = UUID.randomUUID();
		TransactionRequest request = new TransactionRequest();
		request.setAccountNumber(TEST_ACCOUNT);
		request.setRequestUuid(transactionUUID);
		request.setTransactionMetaDataJson(JSON_DATA);
		request.setTransactionAmount(1L);

		TraceableMessage<TransactionRequest> traceable = new TraceableMessage<>();
		traceable.setProducerAit(config.getAitid());
		traceable.setBusinessTaxonomyId(null);
		traceable.setCorrelationId("sdfsdfsdf");
		traceable.setPayload(request);
		traceable.setMessageCreationTime(LocalDateTime.now());
		
		// - Execute
		kafkaProducerDao.produceTransactionMessage(traceable);
		TraceableMessage<ResponseMessage<TransactionRequest,TransactionResponse>>
			response = transactionExchangeQueue.take();

		assertEquals( ResponseMessage.MALFORMED_MESSAGE, response.getPayload().getStatus() );
		assertTrue( response.getPayload().getErrorMessage().contains("Missing Business Taxonomy Id"));
	}

	@Test
	void test_transferFulfillemnt_malformed_missing_createdTime() throws InterruptedException {
		transactionExchangeQueue.clear();
		
		// - setup  -------------------
		UUID transactionUUID = UUID.randomUUID();
		TransactionRequest request = new TransactionRequest();
		request.setAccountNumber(TEST_ACCOUNT);
		request.setRequestUuid(transactionUUID);
		request.setTransactionMetaDataJson(JSON_DATA);
		request.setTransactionAmount(1L);

		TraceableMessage<TransactionRequest> traceable = new TraceableMessage<>();
		traceable.setProducerAit(config.getAitid());
		traceable.setBusinessTaxonomyId(TEST_TAXONOMY_ID);
		traceable.setCorrelationId("sdfsdfsdf");
		traceable.setPayload(request);
		traceable.setMessageCreationTime(null);
		
		// - Execute
		kafkaProducerDao.produceTransactionMessage(traceable);
		TraceableMessage<ResponseMessage<TransactionRequest,TransactionResponse>>
			response = transactionExchangeQueue.take();

		assertEquals( ResponseMessage.MALFORMED_MESSAGE, response.getPayload().getStatus() );
		assertTrue( response.getPayload().getErrorMessage().contains("Missing Message Creation Time"));
	}


}
