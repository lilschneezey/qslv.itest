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
import qslv.common.kafka.ResponseMessage;
import qslv.common.kafka.TraceableMessage;
import qslv.transaction.request.CommitReservationRequest;
import qslv.transaction.request.ReservationRequest;
import qslv.transaction.resource.TransactionResource;
import qslv.transaction.response.CommitReservationResponse;
import qslv.transaction.response.ReservationResponse;
import qslv.util.EnableQuickSilver;

@SpringBootTest
@EnableQuickSilver
class Itest_CommitFulfillment {
			
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
	@Autowired ArrayBlockingQueue<TraceableMessage<ResponseMessage<CommitReservationRequest,CommitReservationResponse>>> commitExchangeQueue;
	
	public static String TEST_ACCOUNT = "TEST_ACCOUNT";
	public static String TEST_TAXONOMY_ID = "9.9.9.9.9";
	public static String VALID_STATUS = "EF";
	public static String INVALID_STATUS = "CL";
	public static String JSON_DATA = "{\"value\": 234934}";
	
	@Test
	void testCommitFulfillment_success() throws Exception {
		commitExchangeQueue.clear();
		
		long start_from_amount = 9999L;
		long reservation_amount = -8888L;
		long commit_amount = -9111;
		long reservation_balance = start_from_amount + reservation_amount;
		long final_balance = start_from_amount + reservation_amount + (commit_amount - reservation_amount);
		UUID requestUuid = UUID.randomUUID();
		
		// - setup --------------------
		jdbcDao.setupAccountBalance(TEST_ACCOUNT, start_from_amount);
		jdbcDao.setupAccount(TEST_ACCOUNT, VALID_STATUS);
		
		HashMap<String, String> headerMap = setup_header();
		
		ReservationRequest reservationRequest = new ReservationRequest();
		reservationRequest.setRequestUuid(requestUuid);
		reservationRequest.setAccountNumber(TEST_ACCOUNT);
		reservationRequest.setAuthorizeAgainstBalance(true);
		reservationRequest.setDebitCardNumber(null);
		reservationRequest.setTransactionAmount(reservation_amount);
		reservationRequest.setTransactionMetaDataJson(JSON_DATA);
		
		// - execute Reservation  ------------------
		ReservationResponse reservationResponse = transactionDao.postReservation(headerMap, reservationRequest);
	
		long dbBalance = jdbcDao.selectBalance(TEST_ACCOUNT);
		assertEquals( reservation_balance, dbBalance );
		
		// - setup  -------------------
		UUID commitUUID = UUID.randomUUID();
		CommitReservationRequest request = new CommitReservationRequest();
		request.setAccountNumber(TEST_ACCOUNT);
		request.setRequestUuid(commitUUID);
		request.setReservationUuid(reservationResponse.getResource().getTransactionUuid());
		request.setTransactionMetaDataJson(JSON_DATA);
		request.setTransactionAmount(commit_amount);

		TraceableMessage<CommitReservationRequest> traceable = new TraceableMessage<>();
		traceable.setProducerAit(config.getAitid());
		traceable.setBusinessTaxonomyId(TEST_TAXONOMY_ID);
		traceable.setCorrelationId("sdfsdfsdf");
		traceable.setPayload(request);
		traceable.setMessageCreationTime(LocalDateTime.now());
		
		// - Execute
		kafkaProducerDao.produceCommitMessage(traceable);
		TraceableMessage<ResponseMessage<CommitReservationRequest,CommitReservationResponse>>
			response = commitExchangeQueue.take();

		// - Verify
		assertNotNull( response );
		assertNotNull( response.getPayload());
		assertEquals( ResponseMessage.SUCCESS, response.getPayload().getStatus() );

		assertNotNull( response.getPayload().getRequest() );
		assertNotNull( response.getPayload().getResponse() );
		
		assertEquals( traceable.getBusinessTaxonomyId(), response.getBusinessTaxonomyId() );
		assertEquals( traceable.getCorrelationId(), response.getCorrelationId() );
		assertEquals( traceable.getProducerAit(), response.getProducerAit() );
		assertEquals( traceable.getMessageCreationTime(), response.getMessageCreationTime());
		assertNotNull( response.getMessageCompletionTime() );	
				
		CommitReservationRequest rrequest = response.getPayload().getRequest();
		CommitReservationResponse rresponse = response.getPayload().getResponse();

		assertEquals( TEST_ACCOUNT, rrequest.getAccountNumber() );
		assertEquals( commitUUID, rrequest.getRequestUuid() );
		assertEquals( request.getReservationUuid(), rrequest.getReservationUuid() );
		assertEquals( JSON_DATA, rrequest.getTransactionMetaDataJson() );
		
		assertEquals( CommitReservationResponse.SUCCESS, rresponse.getStatus() );
		assertEquals( TEST_ACCOUNT, rresponse.getResource().getAccountNumber() );
		assertNull( rresponse.getResource().getDebitCardNumber() );
		assertEquals( commitUUID, rresponse.getResource().getRequestUuid() );
		assertEquals( request.getReservationUuid(), rresponse.getResource().getReservationUuid() );
		assertEquals( final_balance, rresponse.getResource().getRunningBalanceAmount() );
		assertEquals( (commit_amount - reservation_amount), rresponse.getResource().getTransactionAmount() );
		assertEquals( JSON_DATA, rresponse.getResource().getTransactionMetaDataJson() );
		assertEquals( TransactionResource.RESERVATION_COMMIT, rresponse.getResource().getTransactionTypeCode() );
		assertNotNull( rresponse.getResource().getTransactionUuid() );
		
	}
	
	@Test
	void test_transferFulfillemnt_noCommitReservation() throws InterruptedException {
		commitExchangeQueue.clear();
		
		// - setup  -------------------
		UUID commitUUID = UUID.randomUUID();
		CommitReservationRequest request = new CommitReservationRequest();
		request.setAccountNumber(TEST_ACCOUNT);
		request.setRequestUuid(commitUUID);
		request.setReservationUuid(commitUUID);
		request.setTransactionMetaDataJson(JSON_DATA);

		TraceableMessage<CommitReservationRequest> traceable = new TraceableMessage<>();
		traceable.setProducerAit(config.getAitid());
		traceable.setBusinessTaxonomyId(TEST_TAXONOMY_ID);
		traceable.setCorrelationId("sdfsdfsdf");
		traceable.setPayload(request);
		traceable.setMessageCreationTime(LocalDateTime.now());
		
		// - Execute
		kafkaProducerDao.produceCommitMessage(traceable);
		TraceableMessage<ResponseMessage<CommitReservationRequest,CommitReservationResponse>>
			response = commitExchangeQueue.take();

		assertEquals( ResponseMessage.INTERNAL_ERROR, response.getPayload().getStatus() );
	}
	
	@Test
	void test_transferFulfillemnt_malformed_requestUUID() throws InterruptedException {
		commitExchangeQueue.clear();
		
		// - setup  -------------------
		UUID commitUUID = UUID.randomUUID();
		CommitReservationRequest request = new CommitReservationRequest();
		request.setAccountNumber(TEST_ACCOUNT);
		request.setRequestUuid(null);
		request.setReservationUuid(commitUUID);
		request.setTransactionMetaDataJson(JSON_DATA);

		TraceableMessage<CommitReservationRequest> traceable = new TraceableMessage<>();
		traceable.setProducerAit(config.getAitid());
		traceable.setBusinessTaxonomyId(TEST_TAXONOMY_ID);
		traceable.setCorrelationId("sdfsdfsdf");
		traceable.setPayload(request);
		traceable.setMessageCreationTime(LocalDateTime.now());
		
		// - Execute
		kafkaProducerDao.produceCommitMessage(traceable);
		TraceableMessage<ResponseMessage<CommitReservationRequest,CommitReservationResponse>>
			response = commitExchangeQueue.take();

		assertEquals( ResponseMessage.MALFORMED_MESSAGE, response.getPayload().getStatus() );
		assertTrue( response.getPayload().getErrorMessage().contains("Missing From Request UUID"));
	}
	
	@Test
	void test_transferFulfillemnt_malformed_reservationUUID() throws InterruptedException {
		commitExchangeQueue.clear();
		
		// - setup  -------------------
		UUID commitUUID = UUID.randomUUID();
		CommitReservationRequest request = new CommitReservationRequest();
		request.setAccountNumber(TEST_ACCOUNT);
		request.setRequestUuid(commitUUID);
		request.setReservationUuid(null);
		request.setTransactionMetaDataJson(JSON_DATA);

		TraceableMessage<CommitReservationRequest> traceable = new TraceableMessage<>();
		traceable.setProducerAit(config.getAitid());
		traceable.setBusinessTaxonomyId(TEST_TAXONOMY_ID);
		traceable.setCorrelationId("sdfsdfsdf");
		traceable.setPayload(request);
		traceable.setMessageCreationTime(LocalDateTime.now());
		
		// - Execute
		kafkaProducerDao.produceCommitMessage(traceable);
		TraceableMessage<ResponseMessage<CommitReservationRequest,CommitReservationResponse>>
			response = commitExchangeQueue.take();

		assertEquals( ResponseMessage.MALFORMED_MESSAGE, response.getPayload().getStatus() );
		assertTrue( response.getPayload().getErrorMessage().contains("Missing From Reservation UUID"));
	}
	
	@Test
	void test_transferFulfillemnt_malformed_jsonMeta() throws InterruptedException {
		commitExchangeQueue.clear();
		
		// - setup  -------------------
		UUID commitUUID = UUID.randomUUID();
		CommitReservationRequest request = new CommitReservationRequest();
		request.setAccountNumber(TEST_ACCOUNT);
		request.setRequestUuid(commitUUID);
		request.setReservationUuid(commitUUID);
		request.setTransactionMetaDataJson(null);

		TraceableMessage<CommitReservationRequest> traceable = new TraceableMessage<>();
		traceable.setProducerAit(config.getAitid());
		traceable.setBusinessTaxonomyId(TEST_TAXONOMY_ID);
		traceable.setCorrelationId("sdfsdfsdf");
		traceable.setPayload(request);
		traceable.setMessageCreationTime(LocalDateTime.now());
		
		// - Execute
		kafkaProducerDao.produceCommitMessage(traceable);
		TraceableMessage<ResponseMessage<CommitReservationRequest,CommitReservationResponse>>
			response = commitExchangeQueue.take();

		assertEquals( ResponseMessage.MALFORMED_MESSAGE, response.getPayload().getStatus() );
		assertTrue( response.getPayload().getErrorMessage().contains("Missing Meta Data"));
	}
	
	@Test
	void test_transferFulfillemnt_malformed_missing_payload() throws InterruptedException {
		commitExchangeQueue.clear();
		
		// - setup  -------------------
		UUID commitUUID = UUID.randomUUID();
		CommitReservationRequest request = new CommitReservationRequest();
		request.setAccountNumber(TEST_ACCOUNT);
		request.setRequestUuid(commitUUID);
		request.setReservationUuid(commitUUID);
		request.setTransactionMetaDataJson(JSON_DATA);

		TraceableMessage<CommitReservationRequest> traceable = new TraceableMessage<>();
		traceable.setProducerAit(config.getAitid());
		traceable.setBusinessTaxonomyId(TEST_TAXONOMY_ID);
		traceable.setCorrelationId("sdfsdfsdf");
		traceable.setPayload(null);
		traceable.setMessageCreationTime(LocalDateTime.now());
		
		// - Execute
		kafkaProducerDao.produceCommitMessage(traceable);
		TraceableMessage<ResponseMessage<CommitReservationRequest,CommitReservationResponse>>
			response = commitExchangeQueue.take();

		assertEquals( ResponseMessage.MALFORMED_MESSAGE, response.getPayload().getStatus() );
		assertTrue( response.getPayload().getErrorMessage().contains("Missing Fulfillment Message"));
	}
	
	@Test
	void test_transferFulfillemnt_malformed_missing_ait() throws InterruptedException {
		commitExchangeQueue.clear();
		
		// - setup  -------------------
		UUID commitUUID = UUID.randomUUID();
		CommitReservationRequest request = new CommitReservationRequest();
		request.setAccountNumber(TEST_ACCOUNT);
		request.setRequestUuid(commitUUID);
		request.setReservationUuid(commitUUID);
		request.setTransactionMetaDataJson(JSON_DATA);

		TraceableMessage<CommitReservationRequest> traceable = new TraceableMessage<>();
		traceable.setProducerAit(null);
		traceable.setBusinessTaxonomyId(TEST_TAXONOMY_ID);
		traceable.setCorrelationId("sdfsdfsdf");
		traceable.setPayload(request);
		traceable.setMessageCreationTime(LocalDateTime.now());
		
		// - Execute
		kafkaProducerDao.produceCommitMessage(traceable);
		TraceableMessage<ResponseMessage<CommitReservationRequest,CommitReservationResponse>>
			response = commitExchangeQueue.take();

		assertEquals( ResponseMessage.MALFORMED_MESSAGE, response.getPayload().getStatus() );
		assertTrue( response.getPayload().getErrorMessage().contains("Missing Producer AIT Id"));
	}
	
	@Test
	void test_transferFulfillemnt_malformed_missing_correlation() throws InterruptedException {
		commitExchangeQueue.clear();
		
		// - setup  -------------------
		UUID commitUUID = UUID.randomUUID();
		CommitReservationRequest request = new CommitReservationRequest();
		request.setAccountNumber(TEST_ACCOUNT);
		request.setRequestUuid(commitUUID);
		request.setReservationUuid(commitUUID);
		request.setTransactionMetaDataJson(JSON_DATA);

		TraceableMessage<CommitReservationRequest> traceable = new TraceableMessage<>();
		traceable.setProducerAit(config.getAitid());
		traceable.setBusinessTaxonomyId(TEST_TAXONOMY_ID);
		traceable.setCorrelationId(null);
		traceable.setPayload(request);
		traceable.setMessageCreationTime(LocalDateTime.now());
		
		// - Execute
		kafkaProducerDao.produceCommitMessage(traceable);
		TraceableMessage<ResponseMessage<CommitReservationRequest,CommitReservationResponse>>
			response = commitExchangeQueue.take();

		assertEquals( ResponseMessage.MALFORMED_MESSAGE, response.getPayload().getStatus() );
		assertTrue( response.getPayload().getErrorMessage().contains("Missing Correlation Id"));
	}
	
	@Test
	void test_transferFulfillemnt_malformed_missing_taxonomy() throws InterruptedException {
		commitExchangeQueue.clear();
		
		// - setup  -------------------
		UUID commitUUID = UUID.randomUUID();
		CommitReservationRequest request = new CommitReservationRequest();
		request.setAccountNumber(TEST_ACCOUNT);
		request.setRequestUuid(commitUUID);
		request.setReservationUuid(commitUUID);
		request.setTransactionMetaDataJson(JSON_DATA);

		TraceableMessage<CommitReservationRequest> traceable = new TraceableMessage<>();
		traceable.setProducerAit(config.getAitid());
		traceable.setBusinessTaxonomyId(null);
		traceable.setCorrelationId("sdfsdfsdf");
		traceable.setPayload(request);
		traceable.setMessageCreationTime(LocalDateTime.now());
		
		// - Execute
		kafkaProducerDao.produceCommitMessage(traceable);
		TraceableMessage<ResponseMessage<CommitReservationRequest,CommitReservationResponse>>
			response = commitExchangeQueue.take();

		assertEquals( ResponseMessage.MALFORMED_MESSAGE, response.getPayload().getStatus() );
		assertTrue( response.getPayload().getErrorMessage().contains("Missing Business Taxonomy Id"));
	}
	
	@Test
	void test_transferFulfillemnt_malformed_missing_createdTime() throws InterruptedException {
		commitExchangeQueue.clear();
		
		// - setup  -------------------
		UUID commitUUID = UUID.randomUUID();
		CommitReservationRequest request = new CommitReservationRequest();
		request.setAccountNumber(TEST_ACCOUNT);
		request.setRequestUuid(commitUUID);
		request.setReservationUuid(commitUUID);
		request.setTransactionMetaDataJson(JSON_DATA);

		TraceableMessage<CommitReservationRequest> traceable = new TraceableMessage<>();
		traceable.setProducerAit(config.getAitid());
		traceable.setBusinessTaxonomyId(TEST_TAXONOMY_ID);
		traceable.setCorrelationId("sdfsdfsdf");
		traceable.setPayload(request);
		traceable.setMessageCreationTime(null);
		
		// - Execute
		kafkaProducerDao.produceCommitMessage(traceable);
		TraceableMessage<ResponseMessage<CommitReservationRequest,CommitReservationResponse>>
			response = commitExchangeQueue.take();

		assertEquals( ResponseMessage.MALFORMED_MESSAGE, response.getPayload().getStatus() );
		assertTrue( response.getPayload().getErrorMessage().contains("Missing Message Creation Time"));
	}
	
	private HashMap<String, String> setup_header() {
		HashMap<String, String> headerMap = new HashMap<>();
		headerMap.put(TraceableRequest.AIT_ID, config.getAitid());
		headerMap.put(TraceableRequest.BUSINESS_TAXONOMY_ID, TEST_TAXONOMY_ID);
		headerMap.put(TraceableRequest.CORRELATION_ID, UUID.randomUUID().toString());
		headerMap.put(TraceableRequest.ACCEPT_VERSION, ReservationRequest.VERSION_1_0);
		return headerMap;
	}

}
