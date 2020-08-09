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
import qslv.transaction.request.TransactionRequest;
import qslv.transaction.resource.TransactionResource;
import qslv.transaction.response.TransactionResponse;
import qslv.util.EnableQuickSilver;

@SpringBootTest
@EnableQuickSilver
@ComponentScan("qslv.util")
class Itest_Transaction {
			
	@Autowired
	TransactionDao transactionDao;
	@Autowired
	JdbcDao jdbcDao;
	@Autowired
	ConfigProperties config;
	
	public static String TEST_ACCOUNT = "TEST_ACCOUNT";
	public static String TEST_TAXONOMY_ID = "9.9.9.9.9";
	public static String VALID_STATUS = "EF";
	public static String INVALIE_STATUS = "CL";
	
	@Test
	void testPostTransaction_success() throws Exception {
		long start_amount = 9999;
		long test_amount = -8888;
		long expected_balance = start_amount + test_amount;
		String metaDataJson = "{\"value\": 234934}";
		UUID requestUuid = UUID.randomUUID();
		
		// - setup --------------------
		jdbcDao.setupAccountBalance(TEST_ACCOUNT, start_amount);
		jdbcDao.setupAccount(TEST_ACCOUNT, VALID_STATUS);
				
		TransactionRequest request = new TransactionRequest();
		request.setAccountNumber(TEST_ACCOUNT);
		request.setDebitCardNumber(null);
		request.setRequestUuid(requestUuid);
		request.setTransactionAmount(test_amount);
		request.setTransactionMetaDataJson(metaDataJson);
		request.setAuthorizeAgainstBalance(true);
		
		// - execute ------------------
		TransactionResponse response = transactionDao.postTransaction(setup_header(), request);
		
		// - verify -------------------
		assertNotNull(response);
		assertEquals(TransactionResponse.SUCCESS, response.getStatus());
		assertNotNull(response.getTransactions());
		assertEquals(1, response.getTransactions().size());

		long dbBalance = jdbcDao.selectBalance(TEST_ACCOUNT);
		assertEquals( expected_balance, dbBalance );
		
		TransactionResource dbTransaction = jdbcDao.selectTransaction(response.getTransactions().get(0).getTransactionUuid());
		TransactionResource responseTransaction = response.getTransactions().get(0);
		
		// verify original data passed through
		assertEquals( expected_balance, responseTransaction.getRunningBalanceAmount());
		assertEquals( test_amount, responseTransaction.getTransactionAmount());
		assertEquals( TEST_ACCOUNT, responseTransaction.getAccountNumber());
		assertEquals( metaDataJson, responseTransaction.getTransactionMetaDataJson());
		assertEquals( requestUuid, responseTransaction.getRequestUuid());
		assertEquals( TransactionResource.NORMAL, responseTransaction.getTransactionTypeCode() );
		assertNotNull(dbTransaction.getInsertTimestamp());
		
		//verify database matches returned data
		// - service cannot return this: assertNotNull(responseTransaction.getInsertTimestamp());
		assertNotNull(responseTransaction.getTransactionUuid());
		assertNull(dbTransaction.getReservationUuid());
		assertNull(responseTransaction.getDebitCardNumber());
		assertNull(dbTransaction.getDebitCardNumber());
		assertNull(responseTransaction.getReservationUuid());
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
		TransactionResponse idempotent = transactionDao.postTransaction(setup_header(), request);
		
		assertNotNull(idempotent);
		assertEquals(TransactionResponse.SUCCESS, idempotent.getStatus());
		assertNotNull(idempotent.getTransactions());
		assertEquals(1, idempotent.getTransactions().size());
		
		TransactionResource idempotentTransaction = idempotent.getTransactions().get(0);

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
	void testPostTransaction_NSF() throws Exception {
		long start_amount = 1111;
		long test_amount = -8888;
		long expected_balance = start_amount;
		String metaDataJson = "{\"value\": 234934}";
		UUID requestUuid = UUID.randomUUID();
		
		// - setup --------------------
		jdbcDao.setupAccountBalance(TEST_ACCOUNT, start_amount);
		jdbcDao.setupAccount(TEST_ACCOUNT, VALID_STATUS);
				
		TransactionRequest request = new TransactionRequest();
		request.setAccountNumber(TEST_ACCOUNT);
		request.setDebitCardNumber(null);
		request.setRequestUuid(requestUuid);
		request.setTransactionAmount(test_amount);
		request.setTransactionMetaDataJson(metaDataJson);
		request.setAuthorizeAgainstBalance(true);
		
		// - execute ------------------
		TransactionResponse response = transactionDao.postTransaction(setup_header(), request);
		
		// - verify -------------------
		assertNotNull(response);
		assertEquals(TransactionResponse.INSUFFICIENT_FUNDS, response.getStatus());
		assertNotNull(response.getTransactions());
		assertEquals(1, response.getTransactions().size());

		long dbBalance = jdbcDao.selectBalance(TEST_ACCOUNT);
		assertEquals( expected_balance, dbBalance );
		
		TransactionResource dbTransaction = jdbcDao.selectTransaction(response.getTransactions().get(0).getTransactionUuid());
		TransactionResource responseTransaction = response.getTransactions().get(0);
		
		// verify original data passed through
		assertEquals( expected_balance, responseTransaction.getRunningBalanceAmount());
		assertEquals( test_amount, responseTransaction.getTransactionAmount());
		assertEquals( TEST_ACCOUNT, responseTransaction.getAccountNumber());
		assertEquals( metaDataJson, responseTransaction.getTransactionMetaDataJson());
		assertEquals( requestUuid, responseTransaction.getRequestUuid());
		assertEquals( TransactionResource.REJECTED_TRANSACTION, responseTransaction.getTransactionTypeCode() );
		assertNotNull(dbTransaction.getInsertTimestamp());
		
		//verify database matches returned data
		// - service cannot return this: assertNotNull(responseTransaction.getInsertTimestamp());
		assertNotNull(responseTransaction.getTransactionUuid());
		assertNull(responseTransaction.getReservationUuid());
		assertNull(responseTransaction.getDebitCardNumber());
		assertNull(dbTransaction.getReservationUuid());
		assertNull(dbTransaction.getDebitCardNumber());
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
		TransactionResponse idempotent = transactionDao.postTransaction(setup_header(), request);
		
		assertNotNull(idempotent);
		assertEquals(TransactionResponse.INSUFFICIENT_FUNDS, idempotent.getStatus());
		assertNotNull(idempotent.getTransactions());
		assertEquals(1, idempotent.getTransactions().size());
		
		TransactionResource idempotentTransaction = idempotent.getTransactions().get(0);

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
	void testPostTransaction_authorizeFalseSuccess() throws Exception {
		long start_amount = 1111;
		long test_amount = -8888;
		long expected_balance = start_amount + test_amount;
		String metaDataJson = "{\"value\": 234934}";
		UUID requestUuid = UUID.randomUUID();
		
		// - setup --------------------
		jdbcDao.setupAccountBalance(TEST_ACCOUNT, start_amount);
		jdbcDao.setupAccount(TEST_ACCOUNT, VALID_STATUS);
				
		TransactionRequest request = new TransactionRequest();
		request.setAccountNumber(TEST_ACCOUNT);
		request.setDebitCardNumber(null);
		request.setRequestUuid(requestUuid);
		request.setTransactionAmount(test_amount);
		request.setTransactionMetaDataJson(metaDataJson);
		request.setAuthorizeAgainstBalance(false);
		
		// - execute ------------------
		TransactionResponse response = transactionDao.postTransaction(setup_header(), request);
		
		// - verify -------------------
		assertNotNull(response);
		assertEquals(TransactionResponse.SUCCESS, response.getStatus());
		assertNotNull(response.getTransactions());
		assertEquals(1, response.getTransactions().size());

		long dbBalance = jdbcDao.selectBalance(TEST_ACCOUNT);
		assertEquals( expected_balance, dbBalance );
		
		TransactionResource dbTransaction = jdbcDao.selectTransaction(response.getTransactions().get(0).getTransactionUuid());
		TransactionResource responseTransaction = response.getTransactions().get(0);
		
		// verify original data passed through
		assertEquals( expected_balance, responseTransaction.getRunningBalanceAmount());
		assertEquals( test_amount, responseTransaction.getTransactionAmount());
		assertEquals( TEST_ACCOUNT, responseTransaction.getAccountNumber());
		assertEquals( metaDataJson, responseTransaction.getTransactionMetaDataJson());
		assertEquals( requestUuid, responseTransaction.getRequestUuid());
		assertEquals( TransactionResource.NORMAL, responseTransaction.getTransactionTypeCode() );
		assertNotNull(dbTransaction.getInsertTimestamp());
		
		//verify database matches returned data
		// - service cannot return this: assertNotNull(responseTransaction.getInsertTimestamp());
		assertNotNull(responseTransaction.getTransactionUuid());
		assertNull(responseTransaction.getReservationUuid());
		assertNull(responseTransaction.getDebitCardNumber());
		assertNull(dbTransaction.getReservationUuid());
		assertNull(dbTransaction.getDebitCardNumber());
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
		TransactionResponse idempotent = transactionDao.postTransaction(setup_header(), request);
		
		assertNotNull(idempotent);
		assertEquals(TransactionResponse.SUCCESS, idempotent.getStatus());
		assertNotNull(idempotent.getTransactions());
		assertEquals(1, idempotent.getTransactions().size());
		
		TransactionResource idempotentTransaction = idempotent.getTransactions().get(0);

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
	void testPostTransaction_badHeaders_ait() throws Exception {
		HashMap<String, String> headers = setup_header();
		headers.remove(TraceableRequest.AIT_ID);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.postTransaction(headers, setup_dummy_request());} );
	}

	@Test
	void testPostTransaction_badHeaders_taxonomy() throws Exception {
		HashMap<String, String> headers = setup_header();
		headers.remove(TraceableRequest.BUSINESS_TAXONOMY_ID);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.postTransaction(headers, setup_dummy_request());} );
	}
	
	@Test
	void testPostTransaction_badHeaders_correlation() throws Exception {
		HashMap<String, String> headers = setup_header();
		headers.remove(TraceableRequest.CORRELATION_ID);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.postTransaction(headers, setup_dummy_request());} );
	}
	
	@Test
	void testPostTransaction_badHeaders_version() throws Exception {
		HashMap<String, String> headers = setup_header();
		headers.remove(TraceableRequest.ACCEPT_VERSION);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.postTransaction(headers, setup_dummy_request());} );
	}
	
	@Test
	void testPostTransaction_badHeaders_invalid_version() throws Exception {
		HashMap<String, String> headers = setup_header();
		headers.replace(TraceableRequest.ACCEPT_VERSION, "XXX");
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.postTransaction(headers, setup_dummy_request());} );
	}

	@Test
	void testPostTransaction_badRequest_requestuuid() throws Exception {
		TransactionRequest request = setup_dummy_request();
		request.setRequestUuid(null);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.postTransaction(setup_header(),request);} );
	}

	@Test
	void testPostTransaction_badRequest_accountnumber() throws Exception {
		TransactionRequest request = setup_dummy_request();
		request.setAccountNumber(null);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.postTransaction(setup_header(),request);} );
	}

	@Test
	void testPostTransaction_badRequest_json() throws Exception {
		TransactionRequest request = setup_dummy_request();
		request.setTransactionMetaDataJson(null);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.postTransaction(setup_header(),request);} );
	}

	@Test
	void testPostTransaction_badRequest_zeroamount() throws Exception {
		TransactionRequest request = setup_dummy_request();
		request.setTransactionAmount(0L);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.postTransaction(setup_header(),request);} );
	}

	private TransactionRequest setup_dummy_request() {
		TransactionRequest request = new TransactionRequest();
		request.setAccountNumber(TEST_ACCOUNT);
		request.setDebitCardNumber(null);
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
		headerMap.put(TraceableRequest.ACCEPT_VERSION, TransactionRequest.VERSION_1_0);
		return headerMap;
	}

}
