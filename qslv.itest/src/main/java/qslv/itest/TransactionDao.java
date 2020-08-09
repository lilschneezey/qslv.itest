package qslv.itest;

import java.util.Collections;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.web.client.ResourceAccessException;
import qslv.common.TimedResponse;
import qslv.common.TraceableRequest;
import qslv.transaction.request.CancelReservationRequest;
import qslv.transaction.request.CommitReservationRequest;
import qslv.transaction.request.ReservationRequest;
import qslv.transaction.request.TransactionRequest;
import qslv.transaction.request.TransferAndTransactRequest;
import qslv.transaction.response.CancelReservationResponse;
import qslv.transaction.response.CommitReservationResponse;
import qslv.transaction.response.ReservationResponse;
import qslv.transaction.response.TransactionResponse;
import qslv.transaction.response.TransferAndTransactResponse;
import qslv.util.RestClientElapsedTimeSLILogger;

@Repository
public class TransactionDao {
	private static final Logger log = LoggerFactory.getLogger(TransactionDao.class);

	private ParameterizedTypeReference<TimedResponse<TransactionResponse>> transactionReference 
	= new ParameterizedTypeReference<TimedResponse<TransactionResponse>>() {};

	private ParameterizedTypeReference<TimedResponse<ReservationResponse>> reservationReference 
	= new ParameterizedTypeReference<TimedResponse<ReservationResponse>>() {};

	private ParameterizedTypeReference<TimedResponse<CancelReservationResponse>> cancelReference 
	= new ParameterizedTypeReference<TimedResponse<CancelReservationResponse>>() {};

	private ParameterizedTypeReference<TimedResponse<CommitReservationResponse>> commitReference 
	= new ParameterizedTypeReference<TimedResponse<CommitReservationResponse>>() {};

	private ParameterizedTypeReference<TimedResponse<TransferAndTransactResponse>> tandtReference 
	= new ParameterizedTypeReference<TimedResponse<TransferAndTransactResponse>>() {};

		@Autowired
	private ConfigProperties config;
	@Autowired
	RestClientElapsedTimeSLILogger restTimer;
	@Autowired
	private RestTemplateProxy restTemplateProxy;
	@Autowired
	private RetryTemplate retryTemplate;

	public void setConfig(ConfigProperties config) {
		this.config = config;
	}
	public void setTemplate(RestTemplateProxy restTemplateProxy) {
		this.restTemplateProxy = restTemplateProxy;
	}
	public void setRestTimer( RestClientElapsedTimeSLILogger restTimer) {
		this.restTimer=restTimer;
	}
	
	public TransactionResponse postTransaction(final Map<String, String> callingHeaders, final TransactionRequest request) {
		log.trace("postTransaction ENTRY {}", request.toString());

		ResponseEntity<TimedResponse<TransactionResponse>> response;
		try {
			response = retryTemplate.execute(new RetryCallback<ResponseEntity<TimedResponse<TransactionResponse>>, ResourceAccessException>() {
				public ResponseEntity<TimedResponse<TransactionResponse>> doWithRetry( RetryContext context) throws ResourceAccessException {
						return restTemplateProxy.exchange(config.getTransactionUrl(), HttpMethod.POST, 
								new HttpEntity<TransactionRequest>(request, buildHeaders(callingHeaders)), transactionReference);
				} });
		} 
		catch (ResourceAccessException ex ) {
			String msg = String.format("HTTP POST to URL %s with %d retries failed.", config.getTransactionUrl(), config.getRestAttempts());
			log.warn("recordTransaction EXIT {}", msg);
			throw ex;
		}
		catch (Exception ex) {
			log.debug("recordTransaction EXIT {}", ex.getLocalizedMessage());
			throw ex;
		}
		
		log.trace("postTransaction EXIT");
		return response.getBody().getPayload();
	}
	
	public ReservationResponse postReservation(final Map<String, String> callingHeaders, final ReservationRequest request) {
		log.trace("postTransaction ENTRY {}", request.toString());

		ResponseEntity<TimedResponse<ReservationResponse>> response;
		try {
			response = retryTemplate.execute(new RetryCallback<ResponseEntity<TimedResponse<ReservationResponse>>, ResourceAccessException>() {
				public ResponseEntity<TimedResponse<ReservationResponse>> doWithRetry( RetryContext context) throws ResourceAccessException {
						return restTemplateProxy.exchange(config.getReservationUrl(), HttpMethod.POST, 
								new HttpEntity<ReservationRequest>(request, buildHeaders(callingHeaders)), reservationReference);
				} });
		} 
		catch (ResourceAccessException ex ) {
			String msg = String.format("HTTP POST to URL %s with %d retries failed.", config.getReservationUrl(), config.getRestAttempts());
			log.warn("recordTransaction EXIT {}", msg);
			throw ex;
		}
		catch (Exception ex) {
			log.debug("recordTransaction EXIT {}", ex.getLocalizedMessage());
			throw ex;
		}
		
		log.trace("postTransaction EXIT");
		return response.getBody().getPayload();
	}
	public CancelReservationResponse cancelReservation(final Map<String, String> callingHeaders, final CancelReservationRequest request) {
		log.trace("postTransaction ENTRY {}", request.toString());

		ResponseEntity<TimedResponse<CancelReservationResponse>> response;
		try {
			response = retryTemplate.execute(new RetryCallback<ResponseEntity<TimedResponse<CancelReservationResponse>>, ResourceAccessException>() {
				public ResponseEntity<TimedResponse<CancelReservationResponse>> doWithRetry( RetryContext context) throws ResourceAccessException {
						return restTemplateProxy.exchange(config.getCancelReservationUrl(), HttpMethod.POST, 
								new HttpEntity<CancelReservationRequest>(request, buildHeaders(callingHeaders)), cancelReference);
				} });
		} 
		catch (ResourceAccessException ex ) {
			String msg = String.format("HTTP POST to URL %s with %d retries failed.", config.getCancelReservationUrl(), config.getRestAttempts());
			log.warn("recordTransaction EXIT {}", msg);
			throw ex;
		}
		catch (Exception ex) {
			log.debug("recordTransaction EXIT {}", ex.getLocalizedMessage());
			throw ex;
		}
		
		log.trace("postTransaction EXIT");
		return response.getBody().getPayload();
	}
	public CommitReservationResponse commitReservation(final Map<String, String> callingHeaders, final CommitReservationRequest request) {
		log.trace("postTransaction ENTRY {}", request.toString());

		ResponseEntity<TimedResponse<CommitReservationResponse>> response;
		try {
			response = retryTemplate.execute(new RetryCallback<ResponseEntity<TimedResponse<CommitReservationResponse>>, ResourceAccessException>() {
				public ResponseEntity<TimedResponse<CommitReservationResponse>> doWithRetry( RetryContext context) throws ResourceAccessException {
						return restTemplateProxy.exchange(config.getCommitReservationUrl(), HttpMethod.POST, 
								new HttpEntity<CommitReservationRequest>(request, buildHeaders(callingHeaders)), commitReference);
				} });
		} 
		catch (ResourceAccessException ex ) {
			String msg = String.format("HTTP POST to URL %s with %d retries failed.", config.getCommitReservationUrl(), config.getRestAttempts());
			log.warn("recordTransaction EXIT {}", msg);
			throw ex;
		}
		catch (Exception ex) {
			log.debug("recordTransaction EXIT {}", ex.getLocalizedMessage());
			throw ex;
		}
		
		log.trace("postTransaction EXIT");
		return response.getBody().getPayload();
	}
	public TransferAndTransactResponse transferAndTransact(final Map<String, String> callingHeaders, final TransferAndTransactRequest request) {
		log.trace("postTransaction ENTRY {}", request.toString());

		ResponseEntity<TimedResponse<TransferAndTransactResponse>> response;
		try {
			response = retryTemplate.execute(new RetryCallback<ResponseEntity<TimedResponse<TransferAndTransactResponse>>, ResourceAccessException>() {
				public ResponseEntity<TimedResponse<TransferAndTransactResponse>> doWithRetry( RetryContext context) throws ResourceAccessException {
						return restTemplateProxy.exchange(config.getTransferAndTransactUrl(), HttpMethod.POST, 
								new HttpEntity<TransferAndTransactRequest>(request, buildHeaders(callingHeaders)), tandtReference);
				} });
		} 
		catch (ResourceAccessException ex ) {
			String msg = String.format("HTTP POST to URL %s with %d retries failed.", config.getTransferAndTransactUrl(), config.getRestAttempts());
			log.warn("recordTransaction EXIT {}", msg);
			throw ex;
		}
		catch (Exception ex) {
			log.debug("recordTransaction EXIT {}", ex.getLocalizedMessage());
			throw ex;
		}
		
		log.trace("postTransaction EXIT");
		return response.getBody().getPayload();
	}
	private HttpHeaders buildHeaders(final Map<String, String> callingHeaders) {
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON) );
		if (callingHeaders.containsKey(TraceableRequest.AIT_ID))
			headers.add(TraceableRequest.AIT_ID, callingHeaders.get(TraceableRequest.AIT_ID));
		if (callingHeaders.containsKey(TraceableRequest.BUSINESS_TAXONOMY_ID))
			headers.add(TraceableRequest.BUSINESS_TAXONOMY_ID, callingHeaders.get(TraceableRequest.BUSINESS_TAXONOMY_ID));
		if (callingHeaders.containsKey(TraceableRequest.CORRELATION_ID))
			headers.add(TraceableRequest.CORRELATION_ID, callingHeaders.get(TraceableRequest.CORRELATION_ID));
		if (callingHeaders.containsKey(TraceableRequest.ACCEPT_VERSION))
			headers.add(TraceableRequest.ACCEPT_VERSION, callingHeaders.get(TraceableRequest.ACCEPT_VERSION));
		return headers;
	}
}