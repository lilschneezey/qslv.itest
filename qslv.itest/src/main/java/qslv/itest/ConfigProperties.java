package qslv.itest;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "qslv")
public class ConfigProperties {
	private String aitid = "27834";
	private int restConnectionRequestTimeout;
	private int restConnectTimeout;
	private int restTimeout;
	private int restBackoffDelay;
	private int restBackoffDelayMax;
	private int restAttempts;
	private String reservationUrl;
	private String transactionUrl;
	private String reserveFundsUrl;
	private String commitReservationUrl;
	private String cancelReservationUrl;
	private String transferAndTransactUrl;
	private String transferFundsUrl;
	private String kafkaTransferRequestQueue;
	private String kafkaCancelRequestQueue;
	private String kafkaCommitRequestQueue;
	private String kafkaTransactionRequestQueue;
	private String kafkaTransferReplyQueue;
	private String kafkaCancelReplyQueue;
	private String kafkaCommitReplyQueue;
	private String kafkaTransactionReplyQueue;
	private String kafkaTransferFulfillmentDeadLetterQueue;
	private String kafkaConsumerPropertiesPath;
	private String kafkaProducerPropertiesPath;

	public String getAitid() {
		return aitid;
	}

	public void setAitid(String aitid) {
		this.aitid = aitid;
	}

	public int getRestConnectionRequestTimeout() {
		return restConnectionRequestTimeout;
	}

	public void setRestConnectionRequestTimeout(int restConnectionRequestTimeout) {
		this.restConnectionRequestTimeout = restConnectionRequestTimeout;
	}

	public int getRestConnectTimeout() {
		return restConnectTimeout;
	}

	public void setRestConnectTimeout(int restConnectTimeout) {
		this.restConnectTimeout = restConnectTimeout;
	}

	public int getRestTimeout() {
		return restTimeout;
	}

	public void setRestTimeout(int restTimeout) {
		this.restTimeout = restTimeout;
	}

	public int getRestBackoffDelay() {
		return restBackoffDelay;
	}

	public void setRestBackoffDelay(int restBackoffDelay) {
		this.restBackoffDelay = restBackoffDelay;
	}

	public int getRestBackoffDelayMax() {
		return restBackoffDelayMax;
	}

	public void setRestBackoffDelayMax(int restBackoffDelayMax) {
		this.restBackoffDelayMax = restBackoffDelayMax;
	}

	public int getRestAttempts() {
		return restAttempts;
	}

	public void setRestAttempts(int restAttempts) {
		this.restAttempts = restAttempts;
	}

	public String getReservationUrl() {
		return reservationUrl;
	}

	public void setReservationUrl(String reservationUrl) {
		this.reservationUrl = reservationUrl;
	}

	public String getTransactionUrl() {
		return transactionUrl;
	}

	public void setTransactionUrl(String transactionUrl) {
		this.transactionUrl = transactionUrl;
	}

	public String getReserveFundsUrl() {
		return reserveFundsUrl;
	}

	public void setReserveFundsUrl(String reserveFundsUrl) {
		this.reserveFundsUrl = reserveFundsUrl;
	}

	public String getCommitReservationUrl() {
		return commitReservationUrl;
	}

	public void setCommitReservationUrl(String commitReservationUrl) {
		this.commitReservationUrl = commitReservationUrl;
	}

	public String getCancelReservationUrl() {
		return cancelReservationUrl;
	}

	public void setCancelReservationUrl(String cancelReservationUrl) {
		this.cancelReservationUrl = cancelReservationUrl;
	}

	public String getTransferAndTransactUrl() {
		return transferAndTransactUrl;
	}

	public void setTransferAndTransactUrl(String transferAndTransactUrl) {
		this.transferAndTransactUrl = transferAndTransactUrl;
	}

	public String getTransferFundsUrl() {
		return transferFundsUrl;
	}

	public void setTransferFundsUrl(String transferFundsUrl) {
		this.transferFundsUrl = transferFundsUrl;
	}

	public String getKafkaTransferRequestQueue() {
		return kafkaTransferRequestQueue;
	}

	public void setKafkaTransferRequestQueue(String kafkaTransferRequestQueue) {
		this.kafkaTransferRequestQueue = kafkaTransferRequestQueue;
	}

	public String getKafkaConsumerPropertiesPath() {
		return kafkaConsumerPropertiesPath;
	}

	public void setKafkaConsumerPropertiesPath(String kafkaConsumerPropertiesPath) {
		this.kafkaConsumerPropertiesPath = kafkaConsumerPropertiesPath;
	}

	public String getKafkaProducerPropertiesPath() {
		return kafkaProducerPropertiesPath;
	}

	public void setKafkaProducerPropertiesPath(String kafkaProducerPropertiesPath) {
		this.kafkaProducerPropertiesPath = kafkaProducerPropertiesPath;
	}

	public String getKafkaTransferFulfillmentDeadLetterQueue() {
		return kafkaTransferFulfillmentDeadLetterQueue;
	}

	public void setKafkaTransferFulfillmentDeadLetterQueue(String kafkaTransferFulfillmentDeadLetterQueue) {
		this.kafkaTransferFulfillmentDeadLetterQueue = kafkaTransferFulfillmentDeadLetterQueue;
	}

	public String getKafkaCancelRequestQueue() {
		return kafkaCancelRequestQueue;
	}

	public void setKafkaCancelRequestQueue(String kafkaCancelRequestQueue) {
		this.kafkaCancelRequestQueue = kafkaCancelRequestQueue;
	}

	public String getKafkaCommitRequestQueue() {
		return kafkaCommitRequestQueue;
	}

	public void setKafkaCommitRequestQueue(String kafkaCommitRequestQueue) {
		this.kafkaCommitRequestQueue = kafkaCommitRequestQueue;
	}

	public String getKafkaTransactionRequestQueue() {
		return kafkaTransactionRequestQueue;
	}

	public void setKafkaTransactionRequestQueue(String kafkaTransactionRequestQueue) {
		this.kafkaTransactionRequestQueue = kafkaTransactionRequestQueue;
	}

	public String getKafkaTransferReplyQueue() {
		return kafkaTransferReplyQueue;
	}

	public void setKafkaTransferReplyQueue(String kafkaTransferReplyQueue) {
		this.kafkaTransferReplyQueue = kafkaTransferReplyQueue;
	}

	public String getKafkaCancelReplyQueue() {
		return kafkaCancelReplyQueue;
	}

	public void setKafkaCancelReplyQueue(String kafkaCancelReplyQueue) {
		this.kafkaCancelReplyQueue = kafkaCancelReplyQueue;
	}

	public String getKafkaCommitReplyQueue() {
		return kafkaCommitReplyQueue;
	}

	public void setKafkaCommitReplyQueue(String kafkaCommitReplyQueue) {
		this.kafkaCommitReplyQueue = kafkaCommitReplyQueue;
	}

	public String getKafkaTransactionReplyQueue() {
		return kafkaTransactionReplyQueue;
	}

	public void setKafkaTransactionReplyQueue(String kafkaTransactionReplyQueue) {
		this.kafkaTransactionReplyQueue = kafkaTransactionReplyQueue;
	}
	
}
