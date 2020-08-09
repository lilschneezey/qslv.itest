package qslv.itest;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.web.client.HttpClientErrorException.BadRequest;

import qslv.common.TraceableRequest;
import qslv.transaction.request.ReservationRequest;
import qslv.transaction.resource.TransactionResource;
import qslv.transaction.response.ReservationResponse;
import qslv.util.EnableQuickSilver;

@SpringBootTest
@EnableQuickSilver
@ComponentScan("qslv.util")
class Itest_Reservation {
			
	@Autowired
	TransactionDao transactionDao;
	@Autowired
	JdbcDao jdbcDao;
	@Autowired
	ConfigProperties config;
	
	public static String TEST_ACCOUNT = "TEST_ACCOUNT";
	public static String TEST_DEBIT = "TEST_DEBIT_CARD";
	public static String TEST_TAXONOMY_ID = "9.9.9.9.9";
	public static String VALID_STATUS = "EF";
	public static String INVALIE_STATUS = "CL";
	
	@Test
	void testPostReservation_success() throws Exception {
		long start_amount = 9999;
		long test_amount = -8888;
		long expected_balance = start_amount + test_amount;
		String metaDataJson = "{\"value\": 234934}";
		
		// - setup --------------------
		jdbcDao.setupAccountBalance(TEST_ACCOUNT, start_amount);
		jdbcDao.setupAccount(TEST_ACCOUNT, VALID_STATUS);
				
		ReservationRequest request = setup_dummy_request();
		request.setTransactionAmount(test_amount);
		request.setAuthorizeAgainstBalance(true);
		
		// - execute ------------------
		ReservationResponse response = transactionDao.postReservation(setup_header(), request);
		
		// - verify -------------------
		assertNotNull(response);
		assertEquals(ReservationResponse.SUCCESS, response.getStatus());
		assertNotNull(response.getResource());

		long dbBalance = jdbcDao.selectBalance(TEST_ACCOUNT);
		assertEquals( expected_balance, dbBalance );
		
		TransactionResource dbTransaction = jdbcDao.selectTransaction(response.getResource().getTransactionUuid());
		TransactionResource responseTransaction = response.getResource();
		
		// verify original data passed through
		assertEquals( expected_balance, responseTransaction.getRunningBalanceAmount());
		assertEquals( test_amount, responseTransaction.getTransactionAmount());
		assertEquals( TEST_ACCOUNT, responseTransaction.getAccountNumber());
		assertEquals( metaDataJson, responseTransaction.getTransactionMetaDataJson());
		assertEquals( request.getRequestUuid(), responseTransaction.getRequestUuid());
		assertEquals( TransactionResource.RESERVATION, responseTransaction.getTransactionTypeCode() );
		assertNotNull(dbTransaction.getInsertTimestamp());
		
		//verify database matches returned data
		// - service cannot return this: assertNotNull(responseTransaction.getInsertTimestamp());
		assertNotNull(responseTransaction.getTransactionUuid());
		assertNull(dbTransaction.getReservationUuid());
		assertNull(responseTransaction.getReservationUuid());
		assertEquals(dbTransaction.getAccountNumber(), responseTransaction.getAccountNumber());
		assertEquals(dbTransaction.getAccountNumber(), responseTransaction.getAccountNumber());
		assertEquals(dbTransaction.getRequestUuid(), responseTransaction.getRequestUuid());
		assertEquals(dbTransaction.getReservationUuid(), responseTransaction.getReservationUuid());
		assertEquals(dbTransaction.getRunningBalanceAmount(), responseTransaction.getRunningBalanceAmount());
		assertEquals(dbTransaction.getTransactionAmount(), responseTransaction.getTransactionAmount());
		assertEquals(dbTransaction.getTransactionMetaDataJson(), responseTransaction.getTransactionMetaDataJson());
		assertEquals(dbTransaction.getTransactionTypeCode(), responseTransaction.getTransactionTypeCode());
		assertEquals(dbTransaction.getTransactionUuid(), responseTransaction.getTransactionUuid());

		//---------------------------------------
		// Follow up and verify Idempotency works
		// --------------------------------------
		ReservationResponse idempotent = transactionDao.postReservation(setup_header(), request);
		
		assertNotNull(idempotent);
		assertEquals(ReservationResponse.SUCCESS, idempotent.getStatus());
		assertNotNull(idempotent.getResource());
		
		TransactionResource idempotentTransaction = idempotent.getResource();

		assertEquals(responseTransaction.getAccountNumber(), idempotentTransaction.getAccountNumber());
		assertEquals(responseTransaction.getDebitCardNumber(), idempotentTransaction.getDebitCardNumber());
		assertNotNull(idempotentTransaction.getInsertTimestamp());
		assertEquals(responseTransaction.getRequestUuid(), idempotentTransaction.getRequestUuid());
		assertEquals(responseTransaction.getReservationUuid(), idempotentTransaction.getReservationUuid());
		assertEquals(responseTransaction.getRunningBalanceAmount(), idempotentTransaction.getRunningBalanceAmount());
		assertEquals(responseTransaction.getTransactionAmount(), idempotentTransaction.getTransactionAmount());
		assertEquals(responseTransaction.getTransactionMetaDataJson(), idempotentTransaction.getTransactionMetaDataJson());
		assertEquals(responseTransaction.getTransactionTypeCode(), idempotentTransaction.getTransactionTypeCode());
		assertEquals(responseTransaction.getTransactionUuid(), idempotentTransaction.getTransactionUuid());
	}
	
	@Test
	void testPostReservation_NSF() throws Exception {
		long start_amount = 1111;
		long test_amount = -8888;
		long expected_balance = start_amount;
		String metaDataJson = "{\"value\": 234934}";
		
		// - setup --------------------
		jdbcDao.setupAccountBalance(TEST_ACCOUNT, start_amount);
		jdbcDao.setupAccount(TEST_ACCOUNT, VALID_STATUS);
				
		ReservationRequest request = setup_dummy_request();
		request.setTransactionAmount(test_amount);
		request.setAuthorizeAgainstBalance(true);
		
		// - execute ------------------
		ReservationResponse response = transactionDao.postReservation(setup_header(), request);
		
		// - verify -------------------
		assertNotNull(response);
		assertEquals(ReservationResponse.INSUFFICIENT_FUNDS, response.getStatus());
		assertNotNull(response.getResource());

		long dbBalance = jdbcDao.selectBalance(TEST_ACCOUNT);
		assertEquals( expected_balance, dbBalance );
		
		TransactionResource dbTransaction = jdbcDao.selectTransaction(response.getResource().getTransactionUuid());
		TransactionResource responseTransaction = response.getResource();
		
		// verify original data passed through
		assertEquals( expected_balance, responseTransaction.getRunningBalanceAmount());
		assertEquals( test_amount, responseTransaction.getTransactionAmount());
		assertEquals( TEST_ACCOUNT, responseTransaction.getAccountNumber());
		assertEquals( metaDataJson, responseTransaction.getTransactionMetaDataJson());
		assertEquals( request.getRequestUuid(), responseTransaction.getRequestUuid());
		assertEquals( TransactionResource.REJECTED_TRANSACTION, responseTransaction.getTransactionTypeCode() );
		assertNotNull(dbTransaction.getInsertTimestamp());
		
		//verify database matches returned data
		// - service cannot return this: assertNotNull(responseTransaction.getInsertTimestamp());
		assertNotNull(responseTransaction.getTransactionUuid());
		assertNull(responseTransaction.getReservationUuid());
		assertNull(dbTransaction.getReservationUuid());
		assertEquals(dbTransaction.getAccountNumber(), responseTransaction.getAccountNumber());
		assertEquals(dbTransaction.getDebitCardNumber(), responseTransaction.getDebitCardNumber());
		assertEquals(dbTransaction.getRequestUuid(), responseTransaction.getRequestUuid());
		assertEquals(dbTransaction.getReservationUuid(), responseTransaction.getReservationUuid());
		assertEquals(dbTransaction.getRunningBalanceAmount(), responseTransaction.getRunningBalanceAmount());
		assertEquals(dbTransaction.getTransactionAmount(), responseTransaction.getTransactionAmount());
		assertEquals(dbTransaction.getTransactionMetaDataJson(), responseTransaction.getTransactionMetaDataJson());
		assertEquals(dbTransaction.getTransactionTypeCode(), responseTransaction.getTransactionTypeCode());
		assertEquals(dbTransaction.getTransactionUuid(), responseTransaction.getTransactionUuid());

		//---------------------------------------
		// Follow up and verify Idempotency works
		// --------------------------------------
		ReservationResponse idempotent = transactionDao.postReservation(setup_header(), request);
		
		assertNotNull(idempotent);
		assertEquals(ReservationResponse.INSUFFICIENT_FUNDS, idempotent.getStatus());
		assertNotNull(idempotent.getResource());
		
		TransactionResource idempotentTransaction = idempotent.getResource();

		assertEquals(responseTransaction.getAccountNumber(), idempotentTransaction.getAccountNumber());
		assertEquals(responseTransaction.getDebitCardNumber(), idempotentTransaction.getDebitCardNumber());
		assertNotNull(idempotentTransaction.getInsertTimestamp());
		assertEquals(responseTransaction.getRequestUuid(), idempotentTransaction.getRequestUuid());
		assertEquals(responseTransaction.getReservationUuid(), idempotentTransaction.getReservationUuid());
		assertEquals(responseTransaction.getRunningBalanceAmount(), idempotentTransaction.getRunningBalanceAmount());
		assertEquals(responseTransaction.getTransactionAmount(), idempotentTransaction.getTransactionAmount());
		assertEquals(responseTransaction.getTransactionMetaDataJson(), idempotentTransaction.getTransactionMetaDataJson());
		assertEquals(responseTransaction.getTransactionTypeCode(), idempotentTransaction.getTransactionTypeCode());
		assertEquals(responseTransaction.getTransactionUuid(), idempotentTransaction.getTransactionUuid());
	}
	
	@Test
	void testPostReservation_authorizeFalseSuccess() throws Exception {
		long start_amount = 1111;
		long test_amount = -8888;
		long expected_balance = start_amount + test_amount;
		String metaDataJson = "{\"value\": 234934}";
		
		// - setup --------------------
		jdbcDao.setupAccountBalance(TEST_ACCOUNT, start_amount);
		jdbcDao.setupAccount(TEST_ACCOUNT, VALID_STATUS);
				
		ReservationRequest request = setup_dummy_request();
		request.setTransactionAmount(test_amount);
		request.setAuthorizeAgainstBalance(false);
		
		// - execute ------------------
		ReservationResponse response = transactionDao.postReservation(setup_header(), request);
		
		// - verify -------------------
		assertNotNull(response);
		assertEquals(ReservationResponse.SUCCESS, response.getStatus());
		assertNotNull(response.getResource());

		long dbBalance = jdbcDao.selectBalance(TEST_ACCOUNT);
		assertEquals( expected_balance, dbBalance );
		
		TransactionResource dbTransaction = jdbcDao.selectTransaction(response.getResource().getTransactionUuid());
		TransactionResource responseTransaction = response.getResource();
		
		// verify original data passed through
		assertEquals( expected_balance, responseTransaction.getRunningBalanceAmount());
		assertEquals( test_amount, responseTransaction.getTransactionAmount());
		assertEquals( TEST_ACCOUNT, responseTransaction.getAccountNumber());
		assertEquals( metaDataJson, responseTransaction.getTransactionMetaDataJson());
		assertEquals( request.getRequestUuid(), responseTransaction.getRequestUuid());
		assertEquals( TransactionResource.RESERVATION, responseTransaction.getTransactionTypeCode() );
		assertNotNull(dbTransaction.getInsertTimestamp());
		
		//verify database matches returned data
		// - service cannot return this: assertNotNull(responseTransaction.getInsertTimestamp());
		assertNotNull(responseTransaction.getTransactionUuid());
		assertNull(responseTransaction.getReservationUuid());
		assertNull(dbTransaction.getReservationUuid());
		assertEquals(dbTransaction.getAccountNumber(), responseTransaction.getAccountNumber());
		assertEquals(dbTransaction.getDebitCardNumber(), responseTransaction.getDebitCardNumber());
		assertEquals(dbTransaction.getRequestUuid(), responseTransaction.getRequestUuid());
		assertEquals(dbTransaction.getReservationUuid(), responseTransaction.getReservationUuid());
		assertEquals(dbTransaction.getRunningBalanceAmount(), responseTransaction.getRunningBalanceAmount());
		assertEquals(dbTransaction.getTransactionAmount(), responseTransaction.getTransactionAmount());
		assertEquals(dbTransaction.getTransactionMetaDataJson(), responseTransaction.getTransactionMetaDataJson());
		assertEquals(dbTransaction.getTransactionTypeCode(), responseTransaction.getTransactionTypeCode());
		assertEquals(dbTransaction.getTransactionUuid(), responseTransaction.getTransactionUuid());

		//---------------------------------------
		// Follow up and verify Idempotency works
		// --------------------------------------
		ReservationResponse idempotent = transactionDao.postReservation(setup_header(), request);
		
		assertNotNull(idempotent);
		assertEquals(ReservationResponse.SUCCESS, idempotent.getStatus());
		assertNotNull(idempotent.getResource());
		
		TransactionResource idempotentTransaction = idempotent.getResource();

		assertEquals(responseTransaction.getAccountNumber(), idempotentTransaction.getAccountNumber());
		assertEquals(responseTransaction.getDebitCardNumber(), idempotentTransaction.getDebitCardNumber());
		assertNotNull(idempotentTransaction.getInsertTimestamp());
		assertEquals(responseTransaction.getRequestUuid(), idempotentTransaction.getRequestUuid());
		assertEquals(responseTransaction.getReservationUuid(), idempotentTransaction.getReservationUuid());
		assertEquals(responseTransaction.getRunningBalanceAmount(), idempotentTransaction.getRunningBalanceAmount());
		assertEquals(responseTransaction.getTransactionAmount(), idempotentTransaction.getTransactionAmount());
		assertEquals(responseTransaction.getTransactionMetaDataJson(), idempotentTransaction.getTransactionMetaDataJson());
		assertEquals(responseTransaction.getTransactionTypeCode(), idempotentTransaction.getTransactionTypeCode());
		assertEquals(responseTransaction.getTransactionUuid(), idempotentTransaction.getTransactionUuid());
	}
	
	@Test
	void testPostReservation_badHeaders_ait() throws Exception {
		HashMap<String, String> headers = setup_header();
		headers.remove(TraceableRequest.AIT_ID);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.postReservation(headers, setup_dummy_request());} );
	}

	@Test
	void testPostReservation_badHeaders_taxonomy() throws Exception {
		HashMap<String, String> headers = setup_header();
		headers.remove(TraceableRequest.BUSINESS_TAXONOMY_ID);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.postReservation(headers, setup_dummy_request());} );
	}
	
	@Test
	void testPostReservation_badHeaders_correlation() throws Exception {
		HashMap<String, String> headers = setup_header();
		headers.remove(TraceableRequest.CORRELATION_ID);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.postReservation(headers, setup_dummy_request());} );
	}
	
	@Test
	void testPostReservation_badHeaders_version() throws Exception {
		HashMap<String, String> headers = setup_header();
		headers.remove(TraceableRequest.ACCEPT_VERSION);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.postReservation(headers, setup_dummy_request());} );
	}
	
	@Test
	void testPostReservation_badHeaders_invalid_version() throws Exception {
		HashMap<String, String> headers = setup_header();
		headers.replace(TraceableRequest.ACCEPT_VERSION, "XXX");
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.postReservation(headers, setup_dummy_request());} );
	}

	@Test
	void testPostReservation_badRequest_requestuuid() throws Exception {
		ReservationRequest request = setup_dummy_request();
		request.setRequestUuid(null);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.postReservation(setup_header(),request);} );
	}

	@Test
	void testPostReservation_badRequest_accountnumber() throws Exception {
		ReservationRequest request = setup_dummy_request();
		request.setAccountNumber(null);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.postReservation(setup_header(),request);} );
	}

	@Test
	void testPostReservation_badRequest_json() throws Exception {
		ReservationRequest request = setup_dummy_request();
		request.setTransactionMetaDataJson(null);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.postReservation(setup_header(),request);} );
	}

	@Test
	void testPostReservation_badRequest_zeroamount() throws Exception {
		ReservationRequest request = setup_dummy_request();
		request.setTransactionAmount(0L);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.postReservation(setup_header(),request);} );
	}

	private ReservationRequest setup_dummy_request() {
		ReservationRequest request = new ReservationRequest();
		request.setAccountNumber(TEST_ACCOUNT);
		request.setDebitCardNumber(TEST_DEBIT);
		request.setRequestUuid(UUID.randomUUID());
		request.setTransactionAmount(-1111);
		request.setTransactionMetaDataJson("{\"value\": 234934}");
		request.setAuthorizeAgainstBalance(true);
		return request;
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
