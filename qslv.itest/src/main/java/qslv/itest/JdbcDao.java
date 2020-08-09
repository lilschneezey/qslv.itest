package qslv.itest;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.web.server.ResponseStatusException;

import qslv.transaction.resource.TransactionResource;
import qslv.util.ExternalResourceSLI;


@Repository
public class JdbcDao {
	private static final Logger log = LoggerFactory.getLogger(JdbcDao.class);

	@Autowired(required = false)
	private JdbcTemplate jdbcTemplate;

	public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}
	
	public final static String updateBalance_sql = "UPSERT INTO account_balance (account_id, runningBalance_am) values (?,?);";
	
	@ExternalResourceSLI(value="jdbc::setupAccountBalance", ait = "88888", remoteFailures= {DataAccessException.class})
	public void setupAccountBalance(final String accountNumber, long balance) {
		log.trace("setupAccountBalance ENTRY {}", accountNumber);

		// Update Balance
		int rowsUpdated = jdbcTemplate.update(updateBalance_sql, accountNumber, balance);
		if (rowsUpdated == 1) {
			log.debug("upsertBalance New Balance: {}, {}", accountNumber, balance);
		} else {
			log.error("upsertBalance, ERROR=%d rows updated, SQL=%s", rowsUpdated, updateBalance_sql);
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,
					String.format("%d (!= 1) account_balance rows updated.", rowsUpdated));
		}

		log.trace("setupAccountBalance");
		return;
	}

	public final static String upsertAccount_sql = "UPSERT INTO account (account_no, lifecycle_status_cd) values (?,?);";
	@ExternalResourceSLI(value="jdbc::setupAccount", ait = "88888", remoteFailures= {DataAccessException.class})
	public void setupAccount(final String accountNumber, String lifecycleStatusCd) {
		log.trace("setupAccount ENTRY {}", accountNumber);

		// Update Balance
		int rowsUpdated = jdbcTemplate.update(upsertAccount_sql, accountNumber, lifecycleStatusCd);
		if (rowsUpdated == 1) {
			log.debug("setupAccount Account: {}, {}", accountNumber, lifecycleStatusCd);
		} else {
			log.error("setupAccount, ERROR=%d rows updated, SQL=%s", rowsUpdated, upsertAccount_sql);
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,
					String.format("%d (!= 1) account_balance rows updated.", rowsUpdated));
		}

		log.trace("setupAccount");
		return;
	}
	public final static String getBalance_sql = "SELECT runningBalance_am from account_balance where account_id = ?;";

	@ExternalResourceSLI(value="jdbc::selectBalance", ait = "88888", remoteFailures= {DataAccessException.class})
	public long selectBalance(final String accountNumber) {
		log.trace("selectBalance ENTRY");

		long runningBalance_am = 0;
		try {
			runningBalance_am = jdbcTemplate.queryForObject(getBalance_sql, Long.class, accountNumber);
			log.debug("selectBalance Account={} Balance={}", accountNumber, runningBalance_am);
		} catch (EmptyResultDataAccessException e) {
			log.debug("selectBalance Account=%s not found", accountNumber);
		}
		return runningBalance_am;
	}
	
	public final static String selectTransaction_sql = "SELECT transaction_uuid, request_uuid, account_id, debitCard_id, transaction_am, "
			+ "transactiontype_cd, runningbalance_am, reservation_uuid, transactionmetadata_json, insert_tsz "
			+ "from transaction where transaction_uuid=?;";

	@ExternalResourceSLI(value="jdbc::selectTransaction", ait = "88888", remoteFailures= {DataAccessException.class})
	public TransactionResource selectTransaction(UUID transaction_uuid) {

		log.trace("selectTransaction ENTRY");
		List<TransactionResource> resources = jdbcTemplate.query(selectTransaction_sql,
				new RowMapper<TransactionResource>() {
					public TransactionResource mapRow(ResultSet rs, int rowNum) throws SQLException {
						TransactionResource res = new TransactionResource();
						res.setTransactionUuid(rs.getObject(1, UUID.class));
						res.setRequestUuid(rs.getObject(2, UUID.class));
						res.setAccountNumber(rs.getString(3));
						res.setDebitCardNumber(rs.getString(4));
						res.setTransactionAmount(rs.getLong(5));
						res.setTransactionTypeCode(rs.getString(6));
						res.setRunningBalanceAmount(rs.getLong(7));
						res.setReservationUuid(rs.getObject(8, UUID.class));
						res.setTransactionMetaDataJson(rs.getString(9));
						res.setInsertTimestamp(rs.getTimestamp(10));
						return res;
					}
				}, transaction_uuid);
		if (resources.size() != 1) {
			log.debug("selectTransaction, transaction_uuid (%s) not found.", transaction_uuid);
			throw new ResponseStatusException(HttpStatus.NOT_FOUND,
					String.format("transaction_uuid (%s) not found.", transaction_uuid));
		}
		return resources.get(0);

	}
	public final static String selectTransactionByRequest_sql = "SELECT transaction_uuid, request_uuid, account_id, debitCard_id, transaction_am, "
			+ "transactiontype_cd, runningbalance_am, reservation_uuid, transactionmetadata_json, insert_tsz "
			+ "from transaction where request_uuid=? and account_id=?;";

	@ExternalResourceSLI(value="jdbc::selectTransaction", ait = "88888", remoteFailures= {DataAccessException.class})
	public TransactionResource selectTransactionbyRequest(UUID request_uuid, String accountNumber) {

		log.trace("selectTransaction ENTRY");
		List<TransactionResource> resources = jdbcTemplate.query(selectTransactionByRequest_sql,
				new RowMapper<TransactionResource>() {
					public TransactionResource mapRow(ResultSet rs, int rowNum) throws SQLException {
						TransactionResource res = new TransactionResource();
						res.setTransactionUuid(rs.getObject(1, UUID.class));
						res.setRequestUuid(rs.getObject(2, UUID.class));
						res.setAccountNumber(rs.getString(3));
						res.setDebitCardNumber(rs.getString(4));
						res.setTransactionAmount(rs.getLong(5));
						res.setTransactionTypeCode(rs.getString(6));
						res.setRunningBalanceAmount(rs.getLong(7));
						res.setReservationUuid(rs.getObject(8, UUID.class));
						res.setTransactionMetaDataJson(rs.getString(9));
						res.setInsertTimestamp(rs.getTimestamp(10));
						return res;
					}
				}, request_uuid, accountNumber);
		if (resources.size() != 1) {
			log.debug("selectTransaction, transaction_uuid (%s) not found.", request_uuid);
			throw new ResponseStatusException(HttpStatus.NOT_FOUND,
					String.format("transaction_uuid (%s) not found.", request_uuid));
		}
		return resources.get(0);
	}

	public final static String upsertDebit_sql = "UPSERT INTO debit_card (debit_card_no, account_no, lifecycle_status_cd) values (?,?,?);";
	@ExternalResourceSLI(value="jdbc::setupDebit", ait = "88888", remoteFailures= {DataAccessException.class})
	public void setupDebit(String debitNumber, String accountNumber, String statusCode) {
		int rowsUpdated = jdbcTemplate.update(upsertDebit_sql, debitNumber, accountNumber, statusCode);
		if (rowsUpdated == 1) {
			log.debug("setupAccount Account: {} {}, {}", debitNumber, accountNumber, statusCode);
		} else {
			log.error("setupAccount, ERROR=%d rows updated, SQL=%s", rowsUpdated, upsertDebit_sql);
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,
					String.format("%d (!= 1) account_balance rows updated.", rowsUpdated));
		}
		return;
	}
	
	public final static String deleteOverdraft_sql = "DELETE FROM overdraft_instruction where account_no = ?;";
	public void clearOverdraft( String accountNumber ) {
		int rowsUpdated = jdbcTemplate.update(deleteOverdraft_sql, accountNumber);
		log.debug("clearOverdraft rows deleted: {}", rowsUpdated);
		return;
	}

	public final static String addOverdraft_sql = "INSERT INTO overdraft_instruction (account_no,overdraft_account_no,"
			+ "lifecycle_status_cd,effective_start_dt, effective_end_dt, sequence) values (?,?,?,?,?,?);";
	public void addOverdraft( String accountNumber, String odAccountNumber, String statusCode, String startDate, String endDate, int sequence ) {
		int rowsUpdated = jdbcTemplate.update(addOverdraft_sql, accountNumber, odAccountNumber, statusCode,
				startDate, endDate, sequence);
		log.debug("Overdraft row added: {}", rowsUpdated);
		return;
	}
}