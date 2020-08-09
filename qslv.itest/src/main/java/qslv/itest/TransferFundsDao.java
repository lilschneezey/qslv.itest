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
import qslv.transfer.request.TransferFundsRequest;
import qslv.transfer.response.TransferFundsResponse;
import qslv.util.RestClientElapsedTimeSLILogger;

@Repository
public class TransferFundsDao {
	private static final Logger log = LoggerFactory.getLogger(TransferFundsDao.class);
	private ParameterizedTypeReference<TimedResponse<TransferFundsResponse>> typeReference 
		= new ParameterizedTypeReference<TimedResponse<TransferFundsResponse>>() {};

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
	
	public TransferFundsResponse transferFunds(final Map<String, String> callingHeaders, final TransferFundsRequest request) {
		log.trace("transferFunds ENTRY {}", request.toString());

		ResponseEntity<TimedResponse<TransferFundsResponse>> response;
		try {
			response = retryTemplate.execute(new RetryCallback<ResponseEntity<TimedResponse<TransferFundsResponse>>, ResourceAccessException>() {
				public ResponseEntity<TimedResponse<TransferFundsResponse>> doWithRetry( RetryContext context) throws ResourceAccessException {
						return restTemplateProxy.exchange(config.getTransferFundsUrl(), HttpMethod.POST, 
								new HttpEntity<TransferFundsRequest>(request, buildHeaders(callingHeaders)), typeReference);
				} });
		} 
		catch (ResourceAccessException ex ) {
			String msg = String.format("HTTP POST to URL %s with %d retries failed.", config.getTransferFundsUrl(), config.getRestAttempts());
			log.warn("transferFunds EXIT {}", msg);
			throw ex;
		}
		catch (Exception ex) {
			log.debug("transferFunds EXIT {}", ex.getLocalizedMessage());
			throw ex;
		}
		
		//TODO compare remote time vs. local time
		log.trace("transferFunds EXIT");
		return response.getBody().getPayload();
	}
	
	private HttpHeaders buildHeaders(final Map<String, String> callingHeaders) {
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON) );
		if ( callingHeaders.containsKey(TraceableRequest.AIT_ID))
			headers.add(TraceableRequest.AIT_ID, config.getAitid());
		if ( callingHeaders.containsKey(TraceableRequest.BUSINESS_TAXONOMY_ID))
			headers.add(TraceableRequest.BUSINESS_TAXONOMY_ID, callingHeaders.get(TraceableRequest.BUSINESS_TAXONOMY_ID));
		if ( callingHeaders.containsKey(TraceableRequest.CORRELATION_ID))
			headers.add(TraceableRequest.CORRELATION_ID, callingHeaders.get(TraceableRequest.CORRELATION_ID));
		if ( callingHeaders.containsKey(TraceableRequest.ACCEPT_VERSION))
			headers.add(TraceableRequest.ACCEPT_VERSION, callingHeaders.get(TraceableRequest.ACCEPT_VERSION));
		return headers;
	}
}