package qslv.itest;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.web.client.HttpClientErrorException.BadRequest;
import org.springframework.web.client.HttpClientErrorException.Conflict;
import org.springframework.web.client.HttpClientErrorException.NotFound;

import qslv.common.TraceableRequest;
import qslv.transaction.request.CancelReservationRequest;
import qslv.transaction.request.ReservationRequest;
import qslv.transaction.resource.TransactionResource;
import qslv.transaction.response.CancelReservationResponse;
import qslv.transaction.response.ReservationResponse;
import qslv.util.EnableQuickSilver;

@SpringBootTest
@EnableQuickSilver
@ComponentScan("qslv.util")
class Itest_CancelReservation {
			
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
	public String metaDataJson = "{\"value\": 234934}";


	@Test
	void testCancelReservation_success() throws Exception {
		long start_amount = 9999;
		long test_amount = -8888;
		long expected_balance = start_amount + test_amount;
		
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
		//----------------------------------------------------
		//-- Start Cancel
		//----------------------------------------------------
		
		// - Setup--------------------------------------
		CancelReservationRequest cancelRequest = new CancelReservationRequest();
		cancelRequest.setAccountNumber(TEST_ACCOUNT);
		cancelRequest.setRequestUuid(UUID.randomUUID());
		cancelRequest.setReservationUuid(response.getResource().getTransactionUuid());
		cancelRequest.setTransactionMetaDataJson(metaDataJson);	
		
		// - execute --------------------------------------
		CancelReservationResponse cancelResponse = transactionDao.cancelReservation( cancel_header(), cancelRequest);
		
		// - verify --------------------------------------
		assertNotNull(cancelResponse);
		assertEquals(CancelReservationResponse.SUCCESS, response.getStatus());
		assertNotNull(cancelResponse.getResource());
		TransactionResource responseTransaction = cancelResponse.getResource();
		
		TransactionResource dbTransaction = jdbcDao.selectTransaction(cancelResponse.getResource().getTransactionUuid());		
		dbBalance = jdbcDao.selectBalance(TEST_ACCOUNT);
		assertEquals( start_amount, dbBalance );
		
		// verify original data passed through
		assertEquals( start_amount, responseTransaction.getRunningBalanceAmount());
		assertEquals( (0L - test_amount), responseTransaction.getTransactionAmount());
		assertEquals( TEST_ACCOUNT, responseTransaction.getAccountNumber());
		assertEquals( TEST_DEBIT, responseTransaction.getDebitCardNumber());
		assertEquals( metaDataJson, responseTransaction.getTransactionMetaDataJson());
		assertEquals( cancelRequest.getRequestUuid(), responseTransaction.getRequestUuid());
		assertEquals( TransactionResource.RESERVATION_CANCEL, responseTransaction.getTransactionTypeCode() );
		assertEquals( response.getResource().getTransactionUuid(), responseTransaction.getReservationUuid());
		assertNotNull(dbTransaction.getInsertTimestamp());
		assertNotNull(responseTransaction.getTransactionUuid());
		
		//verify database matches returned data
		// - service cannot return this: assertNotNull(responseTransaction.getInsertTimestamp());
		assertEquals(dbTransaction.getAccountNumber(), responseTransaction.getAccountNumber());
		assertEquals(dbTransaction.getDebitCardNumber(), responseTransaction.getDebitCardNumber());
		assertEquals(dbTransaction.getRequestUuid(), responseTransaction.getRequestUuid());
		assertEquals(dbTransaction.getReservationUuid(), responseTransaction.getReservationUuid());
		assertEquals(dbTransaction.getRunningBalanceAmount(), responseTransaction.getRunningBalanceAmount());
		assertEquals(dbTransaction.getTransactionAmount(), responseTransaction.getTransactionAmount());
		assertEquals(dbTransaction.getTransactionMetaDataJson(), responseTransaction.getTransactionMetaDataJson());
		assertEquals(dbTransaction.getTransactionTypeCode(), responseTransaction.getTransactionTypeCode());
		assertEquals(dbTransaction.getTransactionUuid(), responseTransaction.getTransactionUuid());
		assertEquals(dbTransaction.getReservationUuid(), responseTransaction.getReservationUuid());

		//---------------------------------------
		// Follow up and verify Idempotency works
		// --------------------------------------
		CancelReservationResponse idempotent = transactionDao.cancelReservation( cancel_header(), cancelRequest);
		
		assertNotNull(idempotent);
		assertEquals(CancelReservationResponse.SUCCESS, idempotent.getStatus());
		assertNotNull(idempotent.getResource());
		
		TransactionResource idempotentTransaction = idempotent.getResource();

		assertEquals(responseTransaction.getAccountNumber(), idempotentTransaction.getAccountNumber());
		assertEquals(responseTransaction.getDebitCardNumber(), idempotentTransaction.getDebitCardNumber());
		assertTrue(dbTransaction.getInsertTimestamp().toString().contains( idempotentTransaction.getInsertTimestamp().toString() ));
		assertEquals(responseTransaction.getRequestUuid(), idempotentTransaction.getRequestUuid());
		assertEquals(responseTransaction.getReservationUuid(), idempotentTransaction.getReservationUuid());
		assertEquals(responseTransaction.getRunningBalanceAmount(), idempotentTransaction.getRunningBalanceAmount());
		assertEquals(responseTransaction.getTransactionAmount(), idempotentTransaction.getTransactionAmount());
		assertEquals(responseTransaction.getTransactionMetaDataJson(), idempotentTransaction.getTransactionMetaDataJson());
		assertEquals(responseTransaction.getTransactionTypeCode(), idempotentTransaction.getTransactionTypeCode());
		assertEquals(responseTransaction.getTransactionUuid(), idempotentTransaction.getTransactionUuid());
	}

	@Test
	void testCancelReservation_alreadyCanceled() throws Exception {
		long start_amount = 9999;
		long test_amount = -8888;
		long expected_balance = start_amount + test_amount;
		
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
		//----------------------------------------------------
		//-- Start Cancel
		//----------------------------------------------------
		
		// - Setup--------------------------------------
		CancelReservationRequest cancelRequest = new CancelReservationRequest();
		cancelRequest.setAccountNumber(TEST_ACCOUNT);
		cancelRequest.setRequestUuid(UUID.randomUUID());
		cancelRequest.setReservationUuid(response.getResource().getTransactionUuid());
		cancelRequest.setTransactionMetaDataJson(metaDataJson);	
		
		// - execute --------------------------------------
		CancelReservationResponse cancelResponse = transactionDao.cancelReservation( cancel_header(), cancelRequest);
		
		// - verify --------------------------------------
		assertNotNull(cancelResponse);
		assertEquals(CancelReservationResponse.SUCCESS, response.getStatus());
		assertNotNull(cancelResponse.getResource());
		cancelResponse.getResource();
		
		//----------------------------------------------------
		//-- Start Already Canceled
		//----------------------------------------------------
		
		// - Setup--------------------------------------
		CancelReservationRequest secondCancelRequest = new CancelReservationRequest();
		secondCancelRequest.setAccountNumber(TEST_ACCOUNT);
		secondCancelRequest.setRequestUuid(UUID.randomUUID());
		secondCancelRequest.setReservationUuid(response.getResource().getTransactionUuid());
		secondCancelRequest.setTransactionMetaDataJson(metaDataJson);	
		
		// - execute --------------------------------------
		assertThrows( Conflict.class, ()->{ transactionDao.cancelReservation( cancel_header(), secondCancelRequest);} );

	}
	
	@Test
	void testCancelReservation_notPresent() throws Exception {
		// - Setup--------------------------------------
		CancelReservationRequest cancelRequest = new CancelReservationRequest();
		cancelRequest.setAccountNumber(TEST_ACCOUNT);
		cancelRequest.setRequestUuid(UUID.randomUUID());
		cancelRequest.setReservationUuid(UUID.randomUUID());
		cancelRequest.setTransactionMetaDataJson(metaDataJson);	
		
		// - execute --------------------------------------
		assertThrows( NotFound.class, ()->{ transactionDao.cancelReservation( cancel_header(), cancelRequest);} );
	}
	
	@Test
	void testPostReservation_badHeaders_ait() throws Exception {
		HashMap<String, String> headers = setup_header();
		headers.remove(TraceableRequest.AIT_ID);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.cancelReservation(headers, setup_cancel_request());} );
	}

	@Test
	void testPostReservation_badHeaders_taxonomy() throws Exception {
		HashMap<String, String> headers = setup_header();
		headers.remove(TraceableRequest.BUSINESS_TAXONOMY_ID);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.cancelReservation(headers, setup_cancel_request());} );
	}
	
	@Test
	void testPostReservation_badHeaders_correlation() throws Exception {
		HashMap<String, String> headers = setup_header();
		headers.remove(TraceableRequest.CORRELATION_ID);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.cancelReservation(headers, setup_cancel_request());} );
	}
	
	@Test
	void testPostReservation_badHeaders_version() throws Exception {
		HashMap<String, String> headers = setup_header();
		headers.remove(TraceableRequest.ACCEPT_VERSION);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.cancelReservation(headers, setup_cancel_request());} );
	}
	
	@Test
	void testPostReservation_badHeaders_invalid_version() throws Exception {
		HashMap<String, String> headers = setup_header();
		headers.replace(TraceableRequest.ACCEPT_VERSION, "XXX");
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.cancelReservation(headers, setup_cancel_request());} );
	}

	@Test
	void testPostReservation_badRequest_requestuuid() throws Exception {
		CancelReservationRequest request = setup_cancel_request();
		request.setRequestUuid(null);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.cancelReservation(setup_header(),request);} );
	}

	@Test
	void testPostReservation_badRequest_reservationUuid() throws Exception {
		CancelReservationRequest request = setup_cancel_request();
		request.setReservationUuid(null);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.cancelReservation(setup_header(),request);} );
	}

	@Test
	void testPostReservation_badRequest_json() throws Exception {
		CancelReservationRequest request = setup_cancel_request();
		request.setTransactionMetaDataJson(null);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.cancelReservation(setup_header(),request);} );
	}

	private CancelReservationRequest setup_cancel_request() {
		// - Setup--------------------------------------
		CancelReservationRequest cancelRequest = new CancelReservationRequest();
		cancelRequest.setAccountNumber(TEST_ACCOUNT);
		cancelRequest.setRequestUuid(UUID.randomUUID());
		cancelRequest.setReservationUuid(UUID.randomUUID());
		cancelRequest.setTransactionMetaDataJson(metaDataJson);	
		return cancelRequest;
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
	
	private HashMap<String, String> cancel_header() {
		HashMap<String, String> headerMap = new HashMap<>();
		headerMap.put(TraceableRequest.AIT_ID, config.getAitid());
		headerMap.put(TraceableRequest.BUSINESS_TAXONOMY_ID, TEST_TAXONOMY_ID);
		headerMap.put(TraceableRequest.CORRELATION_ID, UUID.randomUUID().toString());
		headerMap.put(TraceableRequest.ACCEPT_VERSION, CancelReservationRequest.VERSION_1_0);
		return headerMap;
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
