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
import qslv.transaction.request.CommitReservationRequest;
import qslv.transaction.request.ReservationRequest;
import qslv.transaction.resource.TransactionResource;
import qslv.transaction.response.CommitReservationResponse;
import qslv.transaction.response.ReservationResponse;
import qslv.util.EnableQuickSilver;

@SpringBootTest
@EnableQuickSilver
@ComponentScan("qslv.util")
class Itest_CommitReservation {
			
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
	void testCommitReservation_success() throws Exception {
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
		//-- Start Commit
		//----------------------------------------------------
		
		// - Setup--------------------------------------
		CommitReservationRequest commitRequest = new CommitReservationRequest();
		commitRequest.setAccountNumber(TEST_ACCOUNT);
		commitRequest.setRequestUuid(UUID.randomUUID());
		commitRequest.setReservationUuid(response.getResource().getTransactionUuid());
		commitRequest.setTransactionMetaDataJson(metaDataJson);	
		commitRequest.setTransactionAmount(test_amount);
		
		// - execute --------------------------------------
		CommitReservationResponse commitResponse = transactionDao.commitReservation( commit_header(), commitRequest);
		
		// - verify --------------------------------------
		assertNotNull(commitResponse);
		assertEquals(CommitReservationResponse.SUCCESS, response.getStatus());
		assertNotNull(commitResponse.getResource());
		TransactionResource responseTransaction = commitResponse.getResource();
		
		TransactionResource dbTransaction = jdbcDao.selectTransaction(commitResponse.getResource().getTransactionUuid());		
		dbBalance = jdbcDao.selectBalance(TEST_ACCOUNT);
		assertEquals( expected_balance, dbBalance );
		
		// verify original data passed through
		assertEquals( expected_balance, responseTransaction.getRunningBalanceAmount());
		assertEquals( 0L, responseTransaction.getTransactionAmount());
		assertEquals( TEST_ACCOUNT, responseTransaction.getAccountNumber());
		assertEquals( TEST_DEBIT, responseTransaction.getDebitCardNumber());
		assertEquals( metaDataJson, responseTransaction.getTransactionMetaDataJson());
		assertEquals( commitRequest.getRequestUuid(), responseTransaction.getRequestUuid());
		assertEquals( TransactionResource.RESERVATION_COMMIT, responseTransaction.getTransactionTypeCode() );
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
		CommitReservationResponse idempotent = transactionDao.commitReservation( commit_header(), commitRequest);
		
		assertNotNull(idempotent);
		assertEquals(CommitReservationResponse.SUCCESS, idempotent.getStatus());
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
	void testCommitReservation_amountDifferentSuccess() throws Exception {
		long start_amount = 9999;
		long test_amount = -8888;
		long additional_amount = -11111;
		long reserved_balance = start_amount + test_amount;
		long final_balance = start_amount + additional_amount;
		
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
		assertEquals( reserved_balance, dbBalance );
		//----------------------------------------------------
		//-- Start Commit
		//----------------------------------------------------
		
		// - Setup--------------------------------------
		CommitReservationRequest commitRequest = new CommitReservationRequest();
		commitRequest.setAccountNumber(TEST_ACCOUNT);
		commitRequest.setRequestUuid(UUID.randomUUID());
		commitRequest.setReservationUuid(response.getResource().getTransactionUuid());
		commitRequest.setTransactionMetaDataJson(metaDataJson);	
		commitRequest.setTransactionAmount(additional_amount);
		
		// - execute --------------------------------------
		CommitReservationResponse commitResponse = transactionDao.commitReservation( commit_header(), commitRequest);
		
		// - verify --------------------------------------
		assertNotNull(commitResponse);
		assertEquals(CommitReservationResponse.SUCCESS, response.getStatus());
		assertNotNull(commitResponse.getResource());
		TransactionResource responseTransaction = commitResponse.getResource();
		
		TransactionResource dbTransaction = jdbcDao.selectTransaction(commitResponse.getResource().getTransactionUuid());		
		dbBalance = jdbcDao.selectBalance(TEST_ACCOUNT);
		assertEquals( final_balance, dbBalance );
		
		// verify original data passed through
		assertEquals( final_balance, responseTransaction.getRunningBalanceAmount());
		assertEquals( (additional_amount - test_amount), responseTransaction.getTransactionAmount());
		assertEquals( TEST_ACCOUNT, responseTransaction.getAccountNumber());
		assertEquals( TEST_DEBIT, responseTransaction.getDebitCardNumber());
		assertEquals( metaDataJson, responseTransaction.getTransactionMetaDataJson());
		assertEquals( commitRequest.getRequestUuid(), responseTransaction.getRequestUuid());
		assertEquals( TransactionResource.RESERVATION_COMMIT, responseTransaction.getTransactionTypeCode() );
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
		CommitReservationResponse idempotent = transactionDao.commitReservation( commit_header(), commitRequest);
		
		assertNotNull(idempotent);
		assertEquals(CommitReservationResponse.SUCCESS, idempotent.getStatus());
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
	void testCommitReservation_alreadyCommited() throws Exception {
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
		//-- Start Commit
		//----------------------------------------------------
		
		// - Setup--------------------------------------
		CommitReservationRequest commitRequest = new CommitReservationRequest();
		commitRequest.setAccountNumber(TEST_ACCOUNT);
		commitRequest.setRequestUuid(UUID.randomUUID());
		commitRequest.setReservationUuid(response.getResource().getTransactionUuid());
		commitRequest.setTransactionMetaDataJson(metaDataJson);	
		commitRequest.setTransactionAmount(test_amount);
		
		// - execute --------------------------------------
		CommitReservationResponse commitResponse = transactionDao.commitReservation( commit_header(), commitRequest);
		
		// - verify --------------------------------------
		assertNotNull(commitResponse);
		assertEquals(CommitReservationResponse.SUCCESS, response.getStatus());
		assertNotNull(commitResponse.getResource());
		commitResponse.getResource();
		
		//----------------------------------------------------
		//-- Start Already Commited
		//----------------------------------------------------
		
		// - Setup--------------------------------------
		CommitReservationRequest secondCommitRequest = new CommitReservationRequest();
		secondCommitRequest.setAccountNumber(TEST_ACCOUNT);
		secondCommitRequest.setRequestUuid(UUID.randomUUID());
		secondCommitRequest.setReservationUuid(response.getResource().getTransactionUuid());
		secondCommitRequest.setTransactionMetaDataJson(metaDataJson);	
		secondCommitRequest.setTransactionAmount(test_amount);

		
		// - execute --------------------------------------
		assertThrows( Conflict.class, ()->{ transactionDao.commitReservation( commit_header(), secondCommitRequest);} );

	}
	
	@Test
	void testCommitReservation_notPresent() throws Exception {
		// - Setup--------------------------------------
		CommitReservationRequest commitRequest = new CommitReservationRequest();
		commitRequest.setAccountNumber(TEST_ACCOUNT);
		commitRequest.setRequestUuid(UUID.randomUUID());
		commitRequest.setReservationUuid(UUID.randomUUID());
		commitRequest.setTransactionMetaDataJson(metaDataJson);
		commitRequest.setTransactionAmount(-999L);
		
		// - execute --------------------------------------
		assertThrows( NotFound.class, ()->{ transactionDao.commitReservation( commit_header(), commitRequest);} );
	}
	
	@Test
	void testPostReservation_badHeaders_ait() throws Exception {
		HashMap<String, String> headers = setup_header();
		headers.remove(TraceableRequest.AIT_ID);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.commitReservation(headers, setup_commit_request());} );
	}

	@Test
	void testPostReservation_badHeaders_taxonomy() throws Exception {
		HashMap<String, String> headers = setup_header();
		headers.remove(TraceableRequest.BUSINESS_TAXONOMY_ID);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.commitReservation(headers, setup_commit_request());} );
	}
	
	@Test
	void testPostReservation_badHeaders_correlation() throws Exception {
		HashMap<String, String> headers = setup_header();
		headers.remove(TraceableRequest.CORRELATION_ID);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.commitReservation(headers, setup_commit_request());} );
	}
	
	@Test
	void testPostReservation_badHeaders_version() throws Exception {
		HashMap<String, String> headers = setup_header();
		headers.remove(TraceableRequest.ACCEPT_VERSION);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.commitReservation(headers, setup_commit_request());} );
	}
	
	@Test
	void testPostReservation_badHeaders_invalid_version() throws Exception {
		HashMap<String, String> headers = setup_header();
		headers.replace(TraceableRequest.ACCEPT_VERSION, "XXX");
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.commitReservation(headers, setup_commit_request());} );
	}

	@Test
	void testPostReservation_badRequest_requestuuid() throws Exception {
		CommitReservationRequest request = setup_commit_request();
		request.setRequestUuid(null);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.commitReservation(setup_header(),request);} );
	}

	@Test
	void testPostReservation_badRequest_reservationUuid() throws Exception {
		CommitReservationRequest request = setup_commit_request();
		request.setReservationUuid(null);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.commitReservation(setup_header(),request);} );
	}

	@Test
	void testPostReservation_badRequest_json() throws Exception {
		CommitReservationRequest request = setup_commit_request();
		request.setTransactionMetaDataJson(null);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.commitReservation(setup_header(),request);} );
	}

	@Test
	void testPostReservation_badRequest_amountZero() throws Exception {
		CommitReservationRequest request = setup_commit_request();
		request.setTransactionAmount(0L);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.commitReservation(setup_header(),request);} );
	}

	private CommitReservationRequest setup_commit_request() {
		// - Setup--------------------------------------
		CommitReservationRequest commitRequest = new CommitReservationRequest();
		commitRequest.setAccountNumber(TEST_ACCOUNT);
		commitRequest.setRequestUuid(UUID.randomUUID());
		commitRequest.setReservationUuid(UUID.randomUUID());
		commitRequest.setTransactionMetaDataJson(metaDataJson);	
		return commitRequest;
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
	
	private HashMap<String, String> commit_header() {
		HashMap<String, String> headerMap = new HashMap<>();
		headerMap.put(TraceableRequest.AIT_ID, config.getAitid());
		headerMap.put(TraceableRequest.BUSINESS_TAXONOMY_ID, TEST_TAXONOMY_ID);
		headerMap.put(TraceableRequest.CORRELATION_ID, UUID.randomUUID().toString());
		headerMap.put(TraceableRequest.ACCEPT_VERSION, CommitReservationRequest.VERSION_1_0);
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
