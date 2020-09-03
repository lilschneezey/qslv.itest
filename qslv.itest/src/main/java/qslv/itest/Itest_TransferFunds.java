package qslv.itest;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.web.client.HttpClientErrorException.BadRequest;
import org.springframework.web.client.HttpClientErrorException.UnprocessableEntity;

import qslv.common.TraceableRequest;
import qslv.transaction.resource.TransactionResource;
import qslv.transfer.request.TransferFulfillmentMessage;
import qslv.transfer.request.TransferFundsRequest;
import qslv.transfer.response.TransferFundsResponse;
import qslv.util.EnableQuickSilver;

@SpringBootTest
@EnableQuickSilver
class Itest_TransferFunds {
			
	@Autowired
	TransferFundsDao transferFundsDao;
	@Autowired
	JdbcDao jdbcDao;
	@Autowired
	ConfigProperties config;
	@Autowired
	KafkaTransferFundsRequestListener kafkaTransferFundsRequestListener;
	@Autowired
	ArrayBlockingQueue<TransferFulfillmentMessage> exchangeQueue;
	
	public static String TEST_FROM_ACCOUNT = "TEST_ACCOUNT";
	public static String TEST_TO_ACCOUNT = "TEST_ACCOUNT1";
	public static String TEST_TAXONOMY_ID = "9.9.9.9.9";
	public static String VALID_STATUS = "EF";
	public static String INVALID_STATUS = "CL";
	public static String JSON_DATA = "{\"value\": 234934}";
	
	@Test
	void testTransferFunds_success() throws Exception {
		exchangeQueue.clear();
		
		long start_from_amount = 9999L;
		long start_to_amount = 4444L;
		long transfer_amount = 8888L;
		long expected_from_balance = start_from_amount - transfer_amount;
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
		TransferFundsResponse response = transferFundsDao.transferFunds(headerMap, request);
		
		// - verify -------------------
		assertNotNull(response);
		assertEquals(TransferFundsResponse.SUCCESS, response.getStatus());
		assertNotNull(response.getReservation());
		TransactionResource reservation = response.getReservation();
		TransferFulfillmentMessage message = response.getFulfillmentMessage();

		long fromBalance = jdbcDao.selectBalance(TEST_FROM_ACCOUNT);
		assertEquals( expected_from_balance, fromBalance );
		
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
		// Kafka Queue Check
		//-----------------------
		TransferFulfillmentMessage kmessage = exchangeQueue.take();
		assertEquals(message.getFromAccountNumber(), kmessage.getFromAccountNumber());
		assertEquals(message.getRequestUuid(), kmessage.getRequestUuid());
		assertEquals(message.getReservationUuid(), kmessage.getReservationUuid());
		assertEquals(message.getToAccountNumber(), kmessage.getToAccountNumber());
		assertEquals(message.getTransactionAmount(), kmessage.getTransactionAmount());
		assertEquals(message.getTransactionMetaDataJson(), kmessage.getTransactionMetaDataJson());

		//-----------------------
		// Start Idempotency Check
		//-----------------------
		//TODO
	}


	@Test
	void testTransferFunds_fromAccount_status_failure() throws Exception {
		exchangeQueue.clear();

		long start_from_amount = 2348234L;
		long start_to_amount = 783492374L;
		long transfer_amount = 8888L;
		UUID requestUuid = UUID.randomUUID();
		
		// - setup --------------------
		jdbcDao.setupAccountBalance(TEST_FROM_ACCOUNT, start_from_amount);
		jdbcDao.setupAccount(TEST_FROM_ACCOUNT, INVALID_STATUS);
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
		assertThrows( UnprocessableEntity.class, ()->{ transferFundsDao.transferFunds(headerMap, request);} );
		long fromBalance = jdbcDao.selectBalance(TEST_FROM_ACCOUNT);
		assertEquals( start_from_amount, fromBalance);
		long toBalance = jdbcDao.selectBalance(TEST_TO_ACCOUNT);
		assertEquals( start_to_amount, toBalance);
	}
	
	@Test
	void testTransferFunds_toAccount_status_failure() throws Exception {
		long start_from_amount = 2348234L;
		long start_to_amount = 783492374L;
		long transfer_amount = 8888L;
		UUID requestUuid = UUID.randomUUID();
		
		// - setup --------------------
		jdbcDao.setupAccountBalance(TEST_FROM_ACCOUNT, start_from_amount);
		jdbcDao.setupAccount(TEST_FROM_ACCOUNT, VALID_STATUS);
		jdbcDao.setupAccountBalance(TEST_TO_ACCOUNT, start_to_amount);
		jdbcDao.setupAccount(TEST_TO_ACCOUNT, INVALID_STATUS);
		
		HashMap<String, String> headerMap = setup_header();
		
		TransferFundsRequest request = new TransferFundsRequest();
		request.setRequestUuid(requestUuid);
		request.setFromAccountNumber(TEST_FROM_ACCOUNT);
		request.setToAccountNumber(TEST_TO_ACCOUNT);
		request.setTransactionAmount(transfer_amount);
		request.setTransactionJsonMetaData(JSON_DATA);
	
		// - execute ------------------
		assertThrows( UnprocessableEntity.class, ()->{ transferFundsDao.transferFunds(headerMap, request);} );
		long fromBalance = jdbcDao.selectBalance(TEST_FROM_ACCOUNT);
		assertEquals( start_from_amount, fromBalance);
		long toBalance = jdbcDao.selectBalance(TEST_TO_ACCOUNT);
		assertEquals( start_to_amount, toBalance);
	}

	@Test
	void testTransferFunds_badHeaders_ait() throws Exception {
		HashMap<String, String> headers = setup_header();
		headers.remove(TraceableRequest.AIT_ID);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transferFundsDao.transferFunds(headers, setup_dummy_request());} );
	}

	@Test
	void testTransferFunds_badHeaders_taxonomy() throws Exception {
		HashMap<String, String> headers = setup_header();
		headers.remove(TraceableRequest.BUSINESS_TAXONOMY_ID);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transferFundsDao.transferFunds(headers, setup_dummy_request());} );
	}
	
	@Test
	void testTransferFunds_badHeaders_correlation() throws Exception {
		HashMap<String, String> headers = setup_header();
		headers.remove(TraceableRequest.CORRELATION_ID);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transferFundsDao.transferFunds(headers, setup_dummy_request());} );
	}
	
	@Test
	void testTransferFunds_badHeaders_version() throws Exception {
		HashMap<String, String> headers = setup_header();
		headers.remove(TraceableRequest.ACCEPT_VERSION);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transferFundsDao.transferFunds(headers, setup_dummy_request());} );
	}
	
	@Test
	void testTransferFunds_badHeaders_invalid_version() throws Exception {
		HashMap<String, String> headers = setup_header();
		headers.replace(TraceableRequest.ACCEPT_VERSION, "XXX");
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transferFundsDao.transferFunds(headers, setup_dummy_request());} );
	}

	@Test
	void testTransferFunds_badRequest_noRequestUUID() throws Exception {
		TransferFundsRequest request = setup_dummy_request();
		request.setRequestUuid(null);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transferFundsDao.transferFunds(setup_header(),request);} );
	}

	@Test
	void testTransferFunds_badRequest_noFromAccount() throws Exception {
		TransferFundsRequest request = setup_dummy_request();
		request.setFromAccountNumber(null);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transferFundsDao.transferFunds(setup_header(),request);} );
	}

	@Test
	void testTransferFunds_badRequest_noToAccount() throws Exception {
		TransferFundsRequest request = setup_dummy_request();
		request.setToAccountNumber(null);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transferFundsDao.transferFunds(setup_header(),request);} );
	}

	@Test
	void testTransferFunds_badRequest_json() throws Exception {
		TransferFundsRequest request = setup_dummy_request();
		request.setTransactionJsonMetaData(null);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transferFundsDao.transferFunds(setup_header(),request);} );
	}

	@Test
	void testTransferFunds_badRequest_ltZeroAmount() throws Exception {
		TransferFundsRequest request = setup_dummy_request();
		request.setTransactionAmount(-1L);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transferFundsDao.transferFunds(setup_header(),request);} );
	}	

	@Test
	void testTransferFunds_badRequest_zeroAmount() throws Exception {
		TransferFundsRequest request = setup_dummy_request();
		request.setTransactionAmount(0L);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transferFundsDao.transferFunds(setup_header(),request);} );
	}	
	
	private TransferFundsRequest setup_dummy_request() {
		TransferFundsRequest request = new TransferFundsRequest();
		request.setRequestUuid(UUID.randomUUID());
		request.setFromAccountNumber(TEST_FROM_ACCOUNT);
		request.setToAccountNumber(TEST_TO_ACCOUNT);
		request.setTransactionAmount(1L);
		request.setTransactionJsonMetaData(JSON_DATA);
		return request;
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
