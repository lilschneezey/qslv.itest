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
import qslv.transaction.request.TransferAndTransactRequest;
import qslv.transaction.resource.TransactionResource;
import qslv.transaction.response.TransactionResponse;
import qslv.transaction.response.TransferAndTransactResponse;
import qslv.util.EnableQuickSilver;

@SpringBootTest
@EnableQuickSilver
@ComponentScan("qslv.util")
class Itest_TransferAndTransact {
			
	@Autowired
	TransactionDao transactionDao;
	@Autowired
	JdbcDao jdbcDao;
	@Autowired
	ConfigProperties config;
	
	public static String TEST_ACCOUNT = "TEST_ACCOUNT";
	public static String TRANSFER_ACCOUNT = "TRANSFER_ACCOUNT";
	public static String DEBIT_ACCOUNT = "DEBIT_CARD_ACCOUNT"; 
	public static String TEST_TAXONOMY_ID = "9.9.9.9.9";
	public static String VALID_STATUS = "EF";
	public static String INVALIE_STATUS = "CL";
	public static String metaDataJson = "{\"value\": 234934}";

	@Test
	void testTransferAndTransact_success() throws Exception {
		long start_amount = 9999;
		long transfer_amount = -3333;
		long expected_balance_with_transfer = start_amount - transfer_amount;
		long test_amount = -4444;
		long expected_balance = start_amount - transfer_amount + test_amount;
		UUID requestUuid = UUID.randomUUID();
		
		// - setup --------------------
		jdbcDao.setupAccountBalance(TEST_ACCOUNT, start_amount);
		jdbcDao.setupAccount(TEST_ACCOUNT, VALID_STATUS);
				
		TransferAndTransactRequest request = new TransferAndTransactRequest();
		request.setRequestUuid(requestUuid);
		request.setTransactionRequest(setup_dummy_request());
		request.setTransferReservation(setup_reservation());
		request.getTransferReservation().setTransactionAmount(transfer_amount);
		request.getTransactionRequest().setTransactionAmount(test_amount);
		
		// - execute ------------------
		TransferAndTransactResponse response = transactionDao.transferAndTransact(setup_header(), request);
		
		// - verify -------------------
		assertNotNull(response);
		assertEquals(TransferAndTransactResponse.SUCCESS, response.getStatus());
		assertNotNull(response.getTransactions());
		assertEquals(2, response.getTransactions().size());

		long dbBalance = jdbcDao.selectBalance(TEST_ACCOUNT);
		assertEquals( expected_balance, dbBalance );
		
		TransactionResource transfer = response.getTransactions().get(0);
		TransactionResource transact = response.getTransactions().get(1);	
		TransactionResource dbtransfer = jdbcDao.selectTransaction(transfer.getTransactionUuid());
		TransactionResource dbtransact = jdbcDao.selectTransaction(transact.getTransactionUuid());
		
		// verify original data on transfer
		assertEquals( expected_balance_with_transfer, transfer.getRunningBalanceAmount());
		assertEquals( (0L - transfer_amount), transfer.getTransactionAmount());
		assertEquals( TEST_ACCOUNT, transfer.getAccountNumber());
		assertEquals( metaDataJson, transfer.getTransactionMetaDataJson());
		assertEquals( requestUuid, transfer.getRequestUuid());
		assertEquals( TransactionResource.NORMAL, transfer.getTransactionTypeCode() );
		
		// verify original data on transact
		assertEquals( expected_balance, transact.getRunningBalanceAmount());
		assertEquals( test_amount, transact.getTransactionAmount());
		assertEquals( TEST_ACCOUNT, transact.getAccountNumber());
		assertEquals( DEBIT_ACCOUNT, transact.getDebitCardNumber());
		assertEquals( metaDataJson, transact.getTransactionMetaDataJson());
		assertEquals( requestUuid, transact.getRequestUuid());
		assertEquals( TransactionResource.NORMAL, transact.getTransactionTypeCode() );

		//verify database matches returned data
		// - service cannot return this: assertNotNull(transfer.getInsertTimestamp());
		assertNotNull(dbtransfer.getInsertTimestamp());
		assertNotNull(transfer.getTransactionUuid());
		assertNull(transfer.getReservationUuid());
		assertNull(transfer.getDebitCardNumber());
		assertNull(dbtransfer.getDebitCardNumber());
		assertNull(dbtransfer.getReservationUuid());
		assertEquals(dbtransfer.getAccountNumber(), transfer.getAccountNumber());
		assertEquals(dbtransfer.getRequestUuid(), transfer.getRequestUuid());
		assertEquals(dbtransfer.getReservationUuid(), transfer.getReservationUuid());
		assertEquals(dbtransfer.getRunningBalanceAmount(), transfer.getRunningBalanceAmount());
		assertEquals(dbtransfer.getTransactionAmount(), transfer.getTransactionAmount());
		assertEquals(dbtransfer.getTransactionMetaDataJson(), transfer.getTransactionMetaDataJson());
		assertEquals(dbtransfer.getTransactionTypeCode(), transfer.getTransactionTypeCode());
		assertEquals(dbtransfer.getTransactionUuid(), transfer.getTransactionUuid());
		
		// - service cannot return this: assertNotNull(transact.getInsertTimestamp());
		assertNotNull(transact.getTransactionUuid());
		assertNull(transact.getReservationUuid());
		assertNotNull(dbtransact.getInsertTimestamp());
		assertNull(dbtransact.getReservationUuid());
		assertEquals(dbtransact.getAccountNumber(), transact.getAccountNumber());
		assertEquals(dbtransact.getDebitCardNumber(), transact.getDebitCardNumber());
		assertEquals(dbtransact.getRequestUuid(), transact.getRequestUuid());
		assertEquals(dbtransact.getReservationUuid(), transact.getReservationUuid());
		assertEquals(dbtransact.getRunningBalanceAmount(), transact.getRunningBalanceAmount());
		assertEquals(dbtransact.getTransactionAmount(), transact.getTransactionAmount());
		assertEquals(dbtransact.getTransactionMetaDataJson(), transact.getTransactionMetaDataJson());
		assertEquals(dbtransact.getTransactionTypeCode(), transact.getTransactionTypeCode());
		assertEquals(dbtransact.getTransactionUuid(), transact.getTransactionUuid());

		//---------------------------------------
		// Follow up and verify Idempotency works
		// --------------------------------------
		TransferAndTransactResponse idempotent = transactionDao.transferAndTransact(setup_header(), request);
		
		assertNotNull(idempotent);
		assertEquals(TransactionResponse.SUCCESS, idempotent.getStatus());
		assertNotNull(idempotent.getTransactions());
		assertEquals(2, idempotent.getTransactions().size());
		
		TransactionResource itransfer = idempotent.getTransactions().get(0);
		TransactionResource itransact = idempotent.getTransactions().get(1);

		assertEquals(transfer.getAccountNumber(), itransfer.getAccountNumber());
		assertEquals(transfer.getDebitCardNumber(), itransfer.getDebitCardNumber());
		assertNotNull(itransfer.getInsertTimestamp());
		assertEquals(transfer.getRequestUuid(), itransfer.getRequestUuid());
		assertEquals(transfer.getReservationUuid(), itransfer.getReservationUuid());
		assertEquals(transfer.getRunningBalanceAmount(), itransfer.getRunningBalanceAmount());
		assertEquals(transfer.getTransactionAmount(), itransfer.getTransactionAmount());
		assertEquals(transfer.getTransactionMetaDataJson(), itransfer.getTransactionMetaDataJson());
		assertEquals(transfer.getTransactionTypeCode(), itransfer.getTransactionTypeCode());
		assertEquals(transfer.getTransactionUuid(), itransfer.getTransactionUuid());

		assertEquals(transact.getAccountNumber(), itransact.getAccountNumber());
		assertEquals(transact.getDebitCardNumber(), itransact.getDebitCardNumber());
		assertNotNull(itransact.getInsertTimestamp());
		assertEquals(transact.getRequestUuid(), itransact.getRequestUuid());
		assertEquals(transact.getReservationUuid(), itransact.getReservationUuid());
		assertEquals(transact.getRunningBalanceAmount(), itransact.getRunningBalanceAmount());
		assertEquals(transact.getTransactionAmount(), itransact.getTransactionAmount());
		assertEquals(transact.getTransactionMetaDataJson(), itransact.getTransactionMetaDataJson());
		assertEquals(transact.getTransactionTypeCode(), itransact.getTransactionTypeCode());
		assertEquals(transact.getTransactionUuid(), itransact.getTransactionUuid());
	}
	
	@Test
	void testTransferAndTransact_badHeaders_ait() throws Exception {
		HashMap<String, String> headers = setup_header();
		headers.remove(TraceableRequest.AIT_ID);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.transferAndTransact(headers, setup_request());} );
	}

	@Test
	void testTransferAndTransact_badHeaders_taxonomy() throws Exception {
		HashMap<String, String> headers = setup_header();
		headers.remove(TraceableRequest.BUSINESS_TAXONOMY_ID);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.transferAndTransact(headers, setup_request());} );
	}
	
	@Test
	void testTransferAndTransact_badHeaders_correlation() throws Exception {
		HashMap<String, String> headers = setup_header();
		headers.remove(TraceableRequest.CORRELATION_ID);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.transferAndTransact(headers, setup_request());} );
	}
	
	@Test
	void testTransferAndTransact_badHeaders_version() throws Exception {
		HashMap<String, String> headers = setup_header();
		headers.remove(TraceableRequest.ACCEPT_VERSION);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.transferAndTransact(headers, setup_request());} );
	}
	
	@Test
	void testTransferAndTransact_badHeaders_invalid_version() throws Exception {
		HashMap<String, String> headers = setup_header();
		headers.replace(TraceableRequest.ACCEPT_VERSION, "XXX");
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.transferAndTransact(headers, setup_request());} );
	}

	@Test
	void testTransferAndTransact_badRequest_requestuuid() throws Exception {
		TransferAndTransactRequest request = setup_request();
		request.setRequestUuid(null);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.transferAndTransact(setup_header(),request);} );
	}

	@Test
	void testTransferAndTransact_badRequest_missing_transfer() throws Exception {
		TransferAndTransactRequest request = setup_request();
		request.setTransferReservation(null);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.transferAndTransact(setup_header(),request);} );
	}

	@Test
	void testTransferAndTransact_badRequest_missing_transfer_uuid() throws Exception {
		TransferAndTransactRequest request = setup_request();
		request.getTransferReservation().setTransactionUuid(null);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.transferAndTransact(setup_header(),request);} );
	}

	@Test
	void testTransferAndTransact_badRequest_positive_transfer_amount() throws Exception {
		TransferAndTransactRequest request = setup_request();
		request.getTransferReservation().setTransactionAmount(1L);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.transferAndTransact(setup_header(),request);} );
	}

	@Test
	void testTransferAndTransact_badRequest_zero_transfer_amount() throws Exception {
		TransferAndTransactRequest request = setup_request();
		request.getTransferReservation().setTransactionAmount(0L);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.transferAndTransact(setup_header(),request);} );
	}
	@Test
	void testTransferAndTransact_badRequest_missing_transact() throws Exception {
		TransferAndTransactRequest request = setup_request();
		request.setTransactionRequest(null);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.transferAndTransact(setup_header(),request);} );
	}

	@Test
	void testTransferAndTransact_badRequest_missing_transact_account() throws Exception {
		TransferAndTransactRequest request = setup_request();
		request.getTransactionRequest().setAccountNumber(null);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.transferAndTransact(setup_header(),request);} );
	}

	@Test
	void testTransferAndTransact_badRequest_missing_transact_json() throws Exception {
		TransferAndTransactRequest request = setup_request();
		request.getTransactionRequest().setTransactionMetaDataJson(null);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.transferAndTransact(setup_header(),request);} );
	}

	@Test
	void testTransferAndTransact_badRequest_zero_transact_amount() throws Exception {
		TransferAndTransactRequest request = setup_request();
		request.getTransactionRequest().setTransactionAmount(0L);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.transferAndTransact(setup_header(),request);} );
	}

	@Test
	void testTransferAndTransact_badRequest_positive_transact_amount() throws Exception {
		TransferAndTransactRequest request = setup_request();
		request.getTransactionRequest().setTransactionAmount(1L);
		// - execute ------------------
		assertThrows( BadRequest.class, ()->{ transactionDao.transferAndTransact(setup_header(),request);} );
	}

	private TransferAndTransactRequest setup_request() {
		TransferAndTransactRequest request = new TransferAndTransactRequest();
		request.setRequestUuid(UUID.randomUUID());
		request.setTransactionRequest(setup_dummy_request());
		request.setTransferReservation(setup_reservation());
		request.getTransferReservation().setTransactionAmount(-1L);
		request.getTransactionRequest().setTransactionAmount(-1L);
		return request;
	}
	private TransactionRequest setup_dummy_request() {
		TransactionRequest request = new TransactionRequest();
		request.setAccountNumber(TEST_ACCOUNT);
		request.setDebitCardNumber(DEBIT_ACCOUNT);
		request.setRequestUuid(UUID.randomUUID());
		request.setTransactionAmount(-9999L);
		request.setTransactionMetaDataJson(metaDataJson);
		request.setAuthorizeAgainstBalance(true);
		return request;
	}
	
	private HashMap<String, String> setup_header() {
		HashMap<String, String> headerMap = new HashMap<>();
		headerMap.put(TraceableRequest.AIT_ID, config.getAitid());
		headerMap.put(TraceableRequest.BUSINESS_TAXONOMY_ID, TEST_TAXONOMY_ID);
		headerMap.put(TraceableRequest.CORRELATION_ID, UUID.randomUUID().toString());
		headerMap.put(TraceableRequest.ACCEPT_VERSION, TransferAndTransactRequest.VERSION_1_0);
		return headerMap;
	}

	private TransactionResource setup_reservation() {
		TransactionResource resource = new TransactionResource();
		resource.setAccountNumber(TRANSFER_ACCOUNT);
		resource.setDebitCardNumber(null);
		resource.setInsertTimestamp(null);
		resource.setRequestUuid(UUID.randomUUID());
		resource.setReservationUuid(null);
		resource.setRunningBalanceAmount(1L);
		resource.setTransactionAmount(-9999L);
		resource.setTransactionMetaDataJson(metaDataJson);
		resource.setTransactionTypeCode(TransactionResource.RESERVATION);
		resource.setTransactionUuid(UUID.randomUUID());
		return resource;
	}

}
