package qslv.itest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import qslv.util.RemoteServiceSLI;

/*
 * In order to use @Annotations to inject service call timings, a proxy bean was needed
 * so Spring could recognize the method call and inject the Aspect/Advice
 */
@Component
public class RestTemplateProxy  {
	@Autowired
	RestTemplate restTemplate;

	public void setRestTemplate(RestTemplate restTemplate) {
		this.restTemplate = restTemplate;
	}

	@RemoteServiceSLI(value="POST /Transaction", ait="55555", remoteAit="11111", remoteFailures= {ResourceAccessException.class})
	public <T> ResponseEntity<T> exchange(String url, HttpMethod method, @Nullable HttpEntity<?> requestEntity, ParameterizedTypeReference<T> responseType, Object... uriVariables) throws RestClientException {
		return restTemplate.exchange(url, method, requestEntity, responseType, uriVariables);
	}
	@RemoteServiceSLI(value="POST /Transaction", ait="66666", remoteAit="22222", remoteFailures= {ResourceAccessException.class})
	public <T> ResponseEntity<T> exchange(String url, HttpMethod method, @Nullable HttpEntity<?> requestEntity, Class<T> responseType, Object... uriVariables) throws RestClientException {
		return restTemplate.exchange(url, method, requestEntity, responseType, uriVariables);
	}
	
}
