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
import qslv.reservefunds.request.ReserveFundsRequest;
import qslv.reservefunds.response.ReserveFundsResponse;
import qslv.util.RestClientElapsedTimeSLILogger;

@Repository
public class ReserveFundsDao {
	private static final Logger log = LoggerFactory.getLogger(ReserveFundsDao.class);
	private ParameterizedTypeReference<TimedResponse<ReserveFundsResponse>> typeReference 
		= new ParameterizedTypeReference<TimedResponse<ReserveFundsResponse>>() {};

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
	
	public ReserveFundsResponse reserveFunds(final Map<String, String> callingHeaders, final ReserveFundsRequest request) {
		log.trace("recordReservation ENTRY {}", request.toString());

		ResponseEntity<TimedResponse<ReserveFundsResponse>> response;
		try {
			response = retryTemplate.execute(new RetryCallback<ResponseEntity<TimedResponse<ReserveFundsResponse>>, ResourceAccessException>() {
				public ResponseEntity<TimedResponse<ReserveFundsResponse>> doWithRetry( RetryContext context) throws ResourceAccessException {
						return restTemplateProxy.exchange(config.getReserveFundsUrl(), HttpMethod.POST, 
								new HttpEntity<ReserveFundsRequest>(request, buildHeaders(callingHeaders)), typeReference);
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
		
		//TODO compare remote time vs. local time
		log.trace("recordReservation EXIT");
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