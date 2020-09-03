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
class Itest_TransOverDraftFulfillment {
			
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
	public static String TEST_ACCOUNT1 = "TEST_ACCOUNT1";
	public static String TEST_TAXONOMY_ID = "9.9.9.9.9";
	public static String VALID_STATUS = "EF";
	public static String INVALID_STATUS = "CL";
	public static String JSON_DATA = "{\"value\": 234934}";
	
	@Test
	void testCancelFulfillment_nofunds() throws Exception {
		transactionExchangeQueue.clear();
		
		long start_from_amount = 1111L;
		long transaction_amount = -8888L;
		
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
		request.setAuthorizeAgainstBalance(true);
		request.setProtectAgainstOverdraft(false);

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
		assertEquals( TransactionResponse.INSUFFICIENT_FUNDS, rresponse.getStatus() );
		assertEquals( TEST_ACCOUNT, rresponse.getTransactions().get(0).getAccountNumber() );
		assertNull( rresponse.getTransactions().get(0).getDebitCardNumber() );
		assertEquals( transactionUUID, rresponse.getTransactions().get(0).getRequestUuid() );
		assertNull( rresponse.getTransactions().get(0).getReservationUuid() );
		assertEquals( start_from_amount, rresponse.getTransactions().get(0).getRunningBalanceAmount() );
		assertEquals( transaction_amount, rresponse.getTransactions().get(0).getTransactionAmount() );
		assertEquals( JSON_DATA, rresponse.getTransactions().get(0).getTransactionMetaDataJson() );
		assertEquals( TransactionResource.REJECTED_TRANSACTION, rresponse.getTransactions().get(0).getTransactionTypeCode() );
		assertNotNull( rresponse.getTransactions().get(0).getTransactionUuid() );

		long dbbalance = jdbcDao.selectBalance(TEST_ACCOUNT);
		assertEquals( dbbalance, start_from_amount );
	}
	
	@Test
	void testCancelFulfillment_oneODsuccess() throws Exception {
		transactionExchangeQueue.clear();
		
		long start_from_amount = 1111L;
		long ample_money = 99999L;
		long transaction_amount = -8888L;
		long expected_from_balance = ample_money + transaction_amount;
		
		// - setup --------------------
		jdbcDao.clearOverdraft(TEST_ACCOUNT);

		jdbcDao.setupAccountBalance(TEST_ACCOUNT, start_from_amount);
		jdbcDao.setupAccount(TEST_ACCOUNT, VALID_STATUS);
		
		jdbcDao.addOverdraft(TEST_ACCOUNT, TEST_ACCOUNT1, VALID_STATUS, "2020/01/01", "2030/01/01", 1);
		jdbcDao.setupAccountBalance(TEST_ACCOUNT1, ample_money);
		jdbcDao.setupAccount(TEST_ACCOUNT1, VALID_STATUS);
		
		// - setup  -------------------
		UUID requestUUID = UUID.randomUUID();
		TransactionRequest request = new TransactionRequest();
		request.setAccountNumber(TEST_ACCOUNT);
		request.setRequestUuid(requestUUID);
		request.setTransactionMetaDataJson(JSON_DATA);
		request.setTransactionAmount(transaction_amount);
		request.setAuthorizeAgainstBalance(true);
		request.setProtectAgainstOverdraft(true);

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
		assertEquals( requestUUID, rrequest.getRequestUuid() );
		assertEquals( JSON_DATA, rrequest.getTransactionMetaDataJson() );

		assertEquals( 5, rresponse.getTransactions().size() );
		TransactionResource rejection = rresponse.getTransactions().get(0);
		TransactionResource reservation = rresponse.getTransactions().get(1);
		TransactionResource transfer = rresponse.getTransactions().get(2);
		TransactionResource transact = rresponse.getTransactions().get(3);
		TransactionResource commit = rresponse.getTransactions().get(4);
		
		// rejection
		assertEquals( TEST_ACCOUNT, rejection.getAccountNumber());
		assertEquals( requestUUID, rejection.getRequestUuid()); //ensure idempotency set up correctly
		assertEquals( transaction_amount, rejection.getTransactionAmount());
		assertEquals( start_from_amount, rejection.getRunningBalanceAmount());
		assertEquals( TransactionResource.REJECTED_TRANSACTION, rejection.getTransactionTypeCode());
		
		// reservation
		assertEquals( TEST_ACCOUNT1, reservation.getAccountNumber());
		assertEquals( requestUUID, reservation.getRequestUuid()); //ensure idempotency set up correctly
		assertEquals( transaction_amount, reservation.getTransactionAmount());
		assertEquals( expected_from_balance, reservation.getRunningBalanceAmount());
		assertEquals( TransactionResource.RESERVATION, reservation.getTransactionTypeCode());

		// transfer
		assertEquals( TEST_ACCOUNT, transfer.getAccountNumber());
		assertEquals( reservation.getTransactionUuid(), transfer.getRequestUuid()); //ensure idempotency set up correctly
		assertEquals( (0L-transaction_amount), transfer.getTransactionAmount());
		assertEquals( (start_from_amount-transaction_amount), transfer.getRunningBalanceAmount());
		assertEquals( TransactionResource.NORMAL, transfer.getTransactionTypeCode());
		
		// transact
		assertEquals( TEST_ACCOUNT, transact.getAccountNumber());
		assertEquals( transfer.getTransactionUuid(), transact.getRequestUuid()); //ensure idempotency set up correctly
		assertEquals( transaction_amount, transact.getTransactionAmount());
		assertEquals( start_from_amount, transact.getRunningBalanceAmount());
		assertEquals( TransactionResource.NORMAL, transact.getTransactionTypeCode());

		// commit
		assertEquals( TEST_ACCOUNT1, commit.getAccountNumber());
		assertEquals( reservation.getTransactionUuid(), commit.getRequestUuid()); //ensure idempotency set up correctly
		assertEquals( 0L, commit.getTransactionAmount());
		assertEquals( expected_from_balance, commit.getRunningBalanceAmount());
		assertEquals( TransactionResource.RESERVATION_COMMIT, commit.getTransactionTypeCode());

		long dbbalance = jdbcDao.selectBalance(TEST_ACCOUNT);
		assertEquals( dbbalance, start_from_amount );

		dbbalance = jdbcDao.selectBalance(TEST_ACCOUNT1);
		assertEquals( dbbalance, expected_from_balance );
	}

}
