package qslv.itest;

import java.util.Collections;

import org.apache.http.impl.client.HttpClients;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import qslv.util.RestClientElapsedTimeSLILogger;

@Configuration
public class RestConfig {
	@Autowired
	private ConfigProperties config;
	
	@Bean
	public RestTemplate restTemplate() {
        HttpComponentsClientHttpRequestFactory httpRequestFactory = new HttpComponentsClientHttpRequestFactory(HttpClients.createDefault());
        httpRequestFactory.setConnectionRequestTimeout(config.getRestConnectionRequestTimeout());;
        httpRequestFactory.setConnectTimeout(config.getRestConnectTimeout());
        httpRequestFactory.setReadTimeout(config.getRestTimeout());

        return new RestTemplate(httpRequestFactory);
	}
	
	@Bean 
	public RetryTemplate retryTemplate() {
		ExponentialBackOffPolicy bop = new ExponentialBackOffPolicy();
		bop.setMaxInterval(config.getRestBackoffDelay());
		bop.setMaxInterval(config.getRestBackoffDelayMax());
		
		SimpleRetryPolicy srp = new SimpleRetryPolicy();
		srp.setMaxAttempts(config.getRestAttempts());
		
		RetryTemplate retryTemplate = new RetryTemplate();
		retryTemplate.setThrowLastExceptionOnExhausted(true);
		retryTemplate.setRetryPolicy(srp);
		retryTemplate.setBackOffPolicy(bop);
		
		return retryTemplate;
	}
	
	@Bean
	public RestClientElapsedTimeSLILogger restTimer() {
		return new RestClientElapsedTimeSLILogger(LoggerFactory.getLogger(TransactionDao.class), config.getAitid(), 
				config.getReservationUrl(), config.getAitid(),
				Collections.singletonList(ResourceAccessException.class)); 
	}
}
