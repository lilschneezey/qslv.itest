package qslv.itest;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.web.client.HttpClientErrorException.BadRequest;
import org.springframework.web.client.HttpClientErrorException.UnprocessableEntity;

import qslv.common.TraceableRequest;
import qslv.reservefunds.request.ReserveFundsRequest;
import qslv.reservefunds.response.ReserveFundsResponse;
import qslv.transaction.resource.TransactionResource;
import qslv.util.EnableQuickSilver;

@SpringBootTest
@EnableQuickSilver
@ComponentScan("qslv.util")
class Itest_ReserveFunds {
			
	@Autowired
	ReserveFundsDao reserveFundsDao;
	@Autowired
	JdbcDao jdbcDao;
	@Autowired
	ConfigProperties config;
	
	public static String TEST_ACCOUNT = "TEST_ACCOUNT";
	public static String TEST_ACCOUNT1 = "TEST_ACCOUNT1";
	public static String TEST_ACCOUNT2 = "TEST_ACCOUNT2";
	public static String TEST_ACCOUNT3 = "TEST_ACCOUNT3";
	public static String TEST_ACCOUNT4 = "TEST_ACCOUNT4";
	public static String TEST_ACCOUNT5 = "TEST_ACCOUNT5";
	public static String TEST_ACCOUNT6 = "TEST_ACCOUNT6";
	public static String TEST_DEBIT = "TEST_DEBIT";
	public static String TEST_TAXONOMY_ID = "9.9.9.9.9";
	public static String VALID_STATUS = "EF";
	public static String INVALID_STATUS = "CL";
	public static String JSON_DATA = "{\"value\": 234934}";
	
	@Test
	void testReserveFunds_account_success() throws Exception {
		long start_amount = 9999;
		long test_amount = -8888;
		long expected_balance = start_amount + test_amount;
		UUID requestUuid = UUID.randomUUID();
		
		// - setup --------------------
		jdbcDao.setupAccountBalance(TEST_ACCOUNT, start_amount);
		jdbcDao.setupAccount(TEST_ACCOUNT, VALID_STATUS);
		
		HashMap<String, String> headerMap = setup_header();
		
		ReserveFundsRequest request = new ReserveFundsRequest();
		request.setAccountNumber(TEST_ACCOUNT);
		request.setDebitCardNumber(null);
		request.setRequestUUID(requestUuid);
		request.setTransactionAmount(test_amount);
		request.setTransactionMetaDataJSON(JSON_DATA);
		request.setProtectAgainstOverdraft(true);
		
		// - execute ------------------
		ReserveFundsResponse response = reserveFundsDao.reserveFunds(headerMap, request);
		
		// - verify -------------------
		assertNotNull(response);
		assertEquals(ReserveFundsResponse.SUCCESS, response.getStatus());
		assertNotNull(response.getTransactions());
		assertEquals(1, response.getTransactions().size());

		long dbBalance = jdbcDao.selectBalance(TEST_ACCOUNT);
		assertEquals( expected_balance, dbBalance );
		TransactionResource responseTransaction = response.getTransactions().get(0);
		
		TransactionResource dbTransaction = jdbcDao.selectTransaction(responseTransaction.getTransactionUuid());
		
		// verify original data passed through
		assertEquals( expected_balance, responseTransaction.getRunningBalanceAmount());
		assertEquals( test_amount, responseTransaction.getTransactionAmount());
		assertEquals( TEST_ACCOUNT, responseTransaction.getAccountNumber());
		assertEquals( JSON_DATA, responseTransaction.getTransactionMetaDataJson());
		assertEquals( requestUuid, responseTransaction.getRequestUuid());
		
		//verify database matches returned data
		assertNotNull(responseTransaction.getTransactionUuid());
		assertNull(responseTransaction.getDebitCardNumber());
		assertNull(responseTransaction.getReservationUuid());
		assertNotNull(dbTransaction.getInsertTimestamp());
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
		
		//-----------------------
		// Start Idempotency Check
		//-----------------------
		ReserveFundsResponse idempotentResponse = reserveFundsDao.reserveFunds(headerMap, request);
		
		assertNotNull(idempotentResponse);
		assertEquals(ReserveFundsResponse.SUCCESS, idempotentResponse.getStatus());
		assertNotNull(idempotentResponse.getTransactions());
		assertEquals(1, idempotentResponse.getTransactions().size());
		
		TransactionResource idempotentTransaction = idempotentResponse.getTransactions().get(0);
		
		assertTransactionsEquals(responseTransaction, idempotentTransaction);
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

	@Test
	void testReserveFunds_account_status_failure() throws Exception {
		long start_amount = 9999;
		long test_amount = -8888;
		UUID requestUuid = UUID.randomUUID();
		
		// - setup --------------------
		jdbcDao.setupAccountBalance(TEST_ACCOUNT, start_amount);
		jdbcDao.setupAccount(TEST_ACCOUNT, INVALID_STATUS);
		
		HashMap<String, String> headerMap = setup_header();
		
		ReserveFundsRequest request = new ReserveFundsRequest();
		request.setAccountNumber(TEST_ACCOUNT);
		request.setDebitCardNumber(null);
		request.setRequestUUID(requestUuid);
		request.setTransactionAmount(test_amount);
		request.setTransactionMetaDataJSON(JSON_DATA);
		request.setProtectAgainstOverdraft(true);
		
		// - execute ------------------
		assertThrows( UnprocessableEntity.class, ()->{ reserveFundsDao.reserveFunds(headerMap, request);} );
		long dbBalance = jdbcDao.selectBalance(TEST_ACCOUNT);
		assertEquals( start_amount, dbBalance);
	}

	@Test
	void testReserveFunds_debit_success() throws Exception {
		long start_amount = 9999;
		long test_amount = -8888;
		long expected_balance = start_amount + test_amount;
		UUID requestUuid = UUID.randomUUID();
		
		// - setup --------------------
		jdbcDao.setupAccountBalance(TEST_ACCOUNT, start_amount);
		jdbcDao.setupAccount(TEST_ACCOUNT, VALID_STATUS);
		jdbcDao.setupDebit(TEST_DEBIT, TEST_ACCOUNT, VALID_STATUS);
		
		HashMap<String, String> headerMap = setup_header();
		
		ReserveFundsRequest request = new ReserveFundsRequest();
		request.setAccountNumber(null);
		request.setDebitCardNumber(TEST_DEBIT);
		request.setRequestUUID(requestUuid);
		request.setTransactionAmount(test_amount);
		request.setTransactionMetaDataJSON(JSON_DATA);
		request.setProtectAgainstOverdraft(true);
		
		// - execute ------------------
		ReserveFundsResponse response = reserveFundsDao.reserveFunds(headerMap, request);
		
		// - verify -------------------
		assertNotNull(response);
		assertEquals(ReserveFundsResponse.SUCCESS, response.getStatus());
		assertNotNull(response.getTransactions());
		assertEquals(1, response.getTransactions().size());

		long dbBalance = jdbcDao.selectBalance(TEST_ACCOUNT);
		assertEquals( expected_balance, dbBalance );
		TransactionResource tran = response.getTransactions().get(0);
		
		TransactionResource dbTransaction = jdbcDao.selectTransaction(tran.getTransactionUuid());
		
		// verify original data passed through
		assertEquals( expected_balance, tran.getRunningBalanceAmount());
		assertEquals( test_amount, tran.getTransactionAmount());
		assertEquals( TEST_ACCOUNT, tran.getAccountNumber());
		assertEquals( TEST_DEBIT, tran.getDebitCardNumber());
		assertEquals( JSON_DATA, tran.getTransactionMetaDataJson());
		assertEquals( requestUuid, tran.getRequestUuid());
		
		//verify database matches returned data
		assertNotNull(tran.getTransactionUuid());
		assertNull(tran.getReservationUuid());
		assertNotNull(dbTransaction.getInsertTimestamp());
		assertNull(dbTransaction.getReservationUuid());
		assertEquals(dbTransaction.getAccountNumber(), tran.getAccountNumber());
		assertEquals(dbTransaction.getDebitCardNumber(), tran.getDebitCardNumber());
		assertEquals(dbTransaction.getRequestUuid(), tran.getRequestUuid());
		assertEquals(dbTransaction.getReservationUuid(), tran.getReservationUuid());
		assertEquals(dbTransaction.getRunningBalanceAmount(), tran.getRunningBalanceAmount());
		assertEquals(dbTransaction.getTransactionAmount(), tran.getTransactionAmount());
		assertEquals(dbTransaction.getTransactionMetaDataJson(), tran.getTransactionMetaDataJson());
		assertEquals(dbTransaction.getTransactionTypeCode(), tran.getTransactionTypeCode());
		assertEquals(dbTransaction.getTransactionUuid(), tran.getTransactionUuid());
	}

	@Test
	void testReserveFunds_account_simpleOD_success() throws Exception {
		long start_amount = 3333L;
		long test_amount = -8888L;
		long ample_money = 99999999L;
		long expected_balance = ample_money + test_amount;
		UUID requestUuid = UUID.randomUUID();
		
		// - setup --------------------
		jdbcDao.setupAccountBalance(TEST_ACCOUNT, start_amount);
		jdbcDao.setupAccount(TEST_ACCOUNT, VALID_STATUS);
		jdbcDao.clearOverdraft(TEST_ACCOUNT);
		
		//-- Sufficient Funds
		jdbcDao.addOverdraft(TEST_ACCOUNT, TEST_ACCOUNT6, VALID_STATUS, "2020/01/01", "2030/01/01", 6);
		jdbcDao.setupAccount(TEST_ACCOUNT6, VALID_STATUS);
		jdbcDao.setupAccountBalance(TEST_ACCOUNT6, ample_money);

		HashMap<String, String> headerMap = setup_header();
		
		ReserveFundsRequest request = new ReserveFundsRequest();
		request.setAccountNumber(TEST_ACCOUNT);
		request.setDebitCardNumber(null);
		request.setRequestUUID(requestUuid);
		request.setTransactionAmount(test_amount);
		request.setTransactionMetaDataJSON(JSON_DATA);
		request.setProtectAgainstOverdraft(true);
		
		// - execute ------------------
		ReserveFundsResponse response = reserveFundsDao.reserveFunds(headerMap, request);
		
		// - verify -------------------
		assertNotNull(response);
		assertEquals(ReserveFundsResponse.SUCCESS_OVERDRAFT, response.getStatus());
		assertNotNull(response.getTransactions());
		assertEquals(2, response.getTransactions().size());

		assertEquals( start_amount, jdbcDao.selectBalance(TEST_ACCOUNT) );
		assertEquals( expected_balance, jdbcDao.selectBalance(TEST_ACCOUNT6) );
		
		assertEquals( TEST_ACCOUNT, response.getTransactions().get(0).getAccountNumber() );
		assertEquals( TEST_ACCOUNT6, response.getTransactions().get(1).getAccountNumber() );

		assertEquals( TransactionResource.REJECTED_TRANSACTION, response.getTransactions().get(0).getTransactionTypeCode() );
		assertEquals( TransactionResource.RESERVATION, response.getTransactions().get(1).getTransactionTypeCode() );

		assertEquals( start_amount, response.getTransactions().get(0).getRunningBalanceAmount() );
		assertEquals( expected_balance, response.getTransactions().get(1).getRunningBalanceAmount() );

		assertEquals( test_amount, response.getTransactions().get(0).getTransactionAmount() );
		assertEquals( test_amount, response.getTransactions().get(1).getTransactionAmount() );

		assertEquals( request.getRequestUUID(), response.getTransactions().get(0).getRequestUuid() );
		assertEquals( request.getRequestUUID(), response.getTransactions().get(1).getRequestUuid() );

		// ------------------------------
		// Start Idempotency Check
		// ------------------------------
		// - execute ------------------
		ReserveFundsResponse idempotentResponse = reserveFundsDao.reserveFunds(headerMap, request);
		assertNotNull(idempotentResponse);
		assertEquals(ReserveFundsResponse.SUCCESS_OVERDRAFT, idempotentResponse.getStatus());
		assertNotNull(idempotentResponse.getTransactions());
		assertEquals(2, idempotentResponse.getTransactions().size());

		assertTransactionsEquals(response.getTransactions().get(0), idempotentResponse.getTransactions().get(0));
		assertTransactionsEquals(response.getTransactions().get(1), idempotentResponse.getTransactions().get(1));
	}
	
	@Test
	void testReserveFunds_account_complexOD_success() throws Exception {
		long start_amount = 3333L;
		long test_amount = -8888L;
		long ample_money = 99999999L;
		long expected_balance = ample_money + test_amount;
		long insuffient = 1L;
		UUID requestUuid = UUID.randomUUID();
		
		// - setup --------------------
		jdbcDao.setupAccountBalance(TEST_ACCOUNT, start_amount);
		jdbcDao.setupAccount(TEST_ACCOUNT, VALID_STATUS);
		jdbcDao.clearOverdraft(TEST_ACCOUNT);
		
		//-- Closed Account
		jdbcDao.addOverdraft(TEST_ACCOUNT, TEST_ACCOUNT1, VALID_STATUS, "2020/01/01", "2030/01/01", 1);
		jdbcDao.setupAccount(TEST_ACCOUNT1, INVALID_STATUS);
		jdbcDao.setupAccountBalance(TEST_ACCOUNT1, ample_money);
		
		//-- Closed Instruction
		jdbcDao.addOverdraft(TEST_ACCOUNT, TEST_ACCOUNT2, INVALID_STATUS, "2020/01/01", "2030/01/01", 2);
		jdbcDao.setupAccount(TEST_ACCOUNT2, VALID_STATUS);
		jdbcDao.setupAccountBalance(TEST_ACCOUNT2, ample_money);

		//-- Expired Instruction
		jdbcDao.addOverdraft(TEST_ACCOUNT, TEST_ACCOUNT3, VALID_STATUS, "2020/01/01", "2020/05/01", 3);
		jdbcDao.setupAccount(TEST_ACCOUNT3, VALID_STATUS);
		jdbcDao.setupAccountBalance(TEST_ACCOUNT3, ample_money);

		//-- Coming Instruction
		jdbcDao.addOverdraft(TEST_ACCOUNT, TEST_ACCOUNT4, VALID_STATUS, "2025/01/01", "2030/01/01", 4);
		jdbcDao.setupAccount(TEST_ACCOUNT4, VALID_STATUS);
		jdbcDao.setupAccountBalance(TEST_ACCOUNT4, ample_money);

		//-- Insufficient Funds
		jdbcDao.addOverdraft(TEST_ACCOUNT, TEST_ACCOUNT5, VALID_STATUS, "2020/01/01", "2030/01/01", 5);
		jdbcDao.setupAccount(TEST_ACCOUNT5, VALID_STATUS);
		jdbcDao.setupAccountBalance(TEST_ACCOUNT5, insuffient);

		//-- Sufficient Funds
		jdbcDao.addOverdraft(TEST_ACCOUNT, TEST_ACCOUNT6, VALID_STATUS, "2020/01/01", "2030/01/01", 6);
		jdbcDao.setupAccount(TEST_ACCOUNT6, VALID_STATUS);
		jdbcDao.setupAccountBalance(TEST_ACCOUNT6, ample_money);

		HashMap<String, String> headerMap = new HashMap<>();
		headerMap.put(TraceableRequest.AIT_ID, config.getAitid());
		headerMap.put(TraceableRequest.BUSINESS_TAXONOMY_ID, TEST_TAXONOMY_ID);
		headerMap.put(TraceableRequest.CORRELATION_ID, UUID.randomUUID().toString());
		headerMap.put(TraceableRequest.ACCEPT_VERSION, ReserveFundsRequest.version1_0);
		
		ReserveFundsRequest request = new ReserveFundsRequest();
		request.setAccountNumber(TEST_ACCOUNT);
		request.setDebitCardNumber(null);
		request.setRequestUUID(requestUuid);
		request.setTransactionAmount(test_amount);
		request.setTransactionMetaDataJSON(JSON_DATA);
		request.setProtectAgainstOverdraft(true);
		
		// - execute ------------------
		ReserveFundsResponse response = reserveFundsDao.reserveFunds(headerMap, request);
		
		// - verify -------------------
		assertNotNull(response);
		assertEquals(ReserveFundsResponse.SUCCESS_OVERDRAFT, response.getStatus());
		assertNotNull(response.getTransactions());
		assertEquals(3, response.getTransactions().size());

		assertEquals( start_amount, jdbcDao.selectBalance(TEST_ACCOUNT) );
		assertEquals( ample_money, jdbcDao.selectBalance(TEST_ACCOUNT1) );
		assertEquals( ample_money, jdbcDao.selectBalance(TEST_ACCOUNT2) );
		assertEquals( ample_money, jdbcDao.selectBalance(TEST_ACCOUNT3) );
		assertEquals( ample_money, jdbcDao.selectBalance(TEST_ACCOUNT4) );
		assertEquals( insuffient, jdbcDao.selectBalance(TEST_ACCOUNT5) );
		assertEquals( expected_balance, jdbcDao.selectBalance(TEST_ACCOUNT6) );
		
		assertEquals( TEST_ACCOUNT, response.getTransactions().get(0).getAccountNumber() );
		assertEquals( TEST_ACCOUNT5, response.getTransactions().get(1).getAccountNumber() );
		assertEquals( TEST_ACCOUNT6, response.getTransactions().get(2).getAccountNumber() );

		assertEquals( TransactionResource.REJECTED_TRANSACTION, response.getTransactions().get(0).getTransactionTypeCode() );
		assertEquals( TransactionResource.REJECTED_TRANSACTION, response.getTransactions().get(1).getTransactionTypeCode() );
		assertEquals( TransactionResource.RESERVATION, response.getTransactions().get(2).getTransactionTypeCode() );

		assertEquals( start_amount, response.getTransactions().get(0).getRunningBalanceAmount() );
		assertEquals( insuffient, response.getTransactions().get(1).getRunningBalanceAmount() );
		assertEquals( expected_balance, response.getTransactions().get(2).getRunningBalanceAmount() );

		assertEquals( test_amount, response.getTransactions().get(0).getTransactionAmount() );
		assertEquals( test_amount, response.getTransactions().get(1).getTransactionAmount() );
		assertEquals( test_amount, response.getTransactions().get(2).getTransactionAmount() );

		assertEquals( request.getRequestUUID(), response.getTransactions().get(0).getRequestUuid() );
		assertEquals( request.getRequestUUID(), response.getTransactions().get(1).getRequestUuid() );
		assertEquals( request.getRequestUUID(), response.getTransactions().get(2).getRequestUuid() );

	}
	
	@Test
	void testReserveFunds_badHeaders_ait() throws Exception {
		HashMap<String, String> headers = setup_header();
		headers.remove(TraceableRequest.AIT_ID);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ reserveFundsDao.reserveFunds(headers, setup_dummy_request());} );
	}

	@Test
	void testReserveFunds_badHeaders_taxonomy() throws Exception {
		HashMap<String, String> headers = setup_header();
		headers.remove(TraceableRequest.BUSINESS_TAXONOMY_ID);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ reserveFundsDao.reserveFunds(headers, setup_dummy_request());} );
	}
	
	@Test
	void testReserveFunds_badHeaders_correlation() throws Exception {
		HashMap<String, String> headers = setup_header();
		headers.remove(TraceableRequest.CORRELATION_ID);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ reserveFundsDao.reserveFunds(headers, setup_dummy_request());} );
	}
	
	@Test
	void testReserveFunds_badHeaders_version() throws Exception {
		HashMap<String, String> headers = setup_header();
		headers.remove(TraceableRequest.ACCEPT_VERSION);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ reserveFundsDao.reserveFunds(headers, setup_dummy_request());} );
	}
	
	@Test
	void testReserveFunds_badHeaders_invalid_version() throws Exception {
		HashMap<String, String> headers = setup_header();
		headers.replace(TraceableRequest.ACCEPT_VERSION, "XXX");
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ reserveFundsDao.reserveFunds(headers, setup_dummy_request());} );
	}

	@Test
	void testReserveFunds_badRequest_noAccountOrDebit() throws Exception {
		ReserveFundsRequest request = setup_dummy_request();
		request.setAccountNumber(null);
		request.setDebitCardNumber(null);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ reserveFundsDao.reserveFunds(setup_header(),request);} );
	}

	@Test
	void testReserveFunds_badRequest_bothAccountAndDebit() throws Exception {
		ReserveFundsRequest request = setup_dummy_request();
		request.setAccountNumber("123");
		request.setDebitCardNumber("123");
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ reserveFundsDao.reserveFunds(setup_header(),request);} );
	}

	@Test
	void testReserveFunds_badRequest_json() throws Exception {
		ReserveFundsRequest request = setup_dummy_request();
		request.setTransactionMetaDataJSON(null);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ reserveFundsDao.reserveFunds(setup_header(),request);} );
	}

	@Test
	void testReserveFunds_badRequest_zeroamount() throws Exception {
		ReserveFundsRequest request = setup_dummy_request();
		request.setTransactionAmount(0L);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ reserveFundsDao.reserveFunds(setup_header(),request);} );
	}	
	
	private ReserveFundsRequest setup_dummy_request() {
		ReserveFundsRequest request = new ReserveFundsRequest();
		request.setAccountNumber(TEST_ACCOUNT);
		request.setDebitCardNumber(null);
		request.setRequestUUID(UUID.randomUUID());
		request.setTransactionAmount(-1L);
		request.setTransactionMetaDataJSON(JSON_DATA);
		request.setProtectAgainstOverdraft(true);
		return request;
	}

	private HashMap<String, String> setup_header() {
		HashMap<String, String> headerMap = new HashMap<>();
		headerMap.put(TraceableRequest.AIT_ID, config.getAitid());
		headerMap.put(TraceableRequest.BUSINESS_TAXONOMY_ID, TEST_TAXONOMY_ID);
		headerMap.put(TraceableRequest.CORRELATION_ID, UUID.randomUUID().toString());
		headerMap.put(TraceableRequest.ACCEPT_VERSION, ReserveFundsRequest.version1_0);
		return headerMap;
	}

}
