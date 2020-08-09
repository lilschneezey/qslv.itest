package qslv.itest;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import javax.annotation.Resource;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import qslv.common.kafka.JacksonAvroDeserializer;
import qslv.common.kafka.ResponseMessage;
import qslv.common.kafka.TraceableMessage;
import qslv.transaction.request.CancelReservationRequest;
import qslv.transaction.request.CommitReservationRequest;
import qslv.transaction.request.TransactionRequest;
import qslv.transaction.response.CancelReservationResponse;
import qslv.transaction.response.CommitReservationResponse;
import qslv.transaction.response.TransactionResponse;
import qslv.transfer.request.TransferFulfillmentMessage;

@Configuration
@EnableKafka
public class KafkaListenerConfig {

	@Autowired
	ConfigProperties config;

	@Resource(name="listenerConfig")
	public Map<String,Object> listenerConfig;	
	
	@Bean
	ArrayBlockingQueue<TransferFulfillmentMessage> transferFundsRequestexchangeQueue() {
		return new ArrayBlockingQueue<>(25);
	}
	@Bean
	ArrayBlockingQueue<TraceableMessage<ResponseMessage<CancelReservationRequest,CancelReservationResponse>>> cancelExchangeQueue() {
		return new ArrayBlockingQueue<>(25);
	}
	@Bean
	ArrayBlockingQueue<TraceableMessage<ResponseMessage<CommitReservationRequest,CommitReservationResponse>>> commitExchangeQueue() {
		return new ArrayBlockingQueue<>(25);
	}
	@Bean
	ArrayBlockingQueue<TraceableMessage<ResponseMessage<TransactionRequest,TransactionResponse>>> transactionExchangeQueue() {
		return new ArrayBlockingQueue<>(25);
	}
	@Bean
	ArrayBlockingQueue<String> deadLetterExchangeQueue() {
		return new ArrayBlockingQueue<String>(25);
	}

	//--TransferFundsRequest Message Consumer
    @Bean
    public ConsumerFactory<String, TraceableMessage<TransferFulfillmentMessage>> consumerFactory() throws Exception {
    	
    	JacksonAvroDeserializer<TraceableMessage<TransferFulfillmentMessage>> jad = new JacksonAvroDeserializer<>();
    	jad.configure(listenerConfig);
    	
        return new DefaultKafkaConsumerFactory<String, TraceableMessage<TransferFulfillmentMessage>>(listenerConfig, new StringDeserializer(),  jad);
    }
    
    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, TraceableMessage<TransferFulfillmentMessage>>> kafkaListenerContainerFactory() throws Exception {
    
        ConcurrentKafkaListenerContainerFactory<String, TraceableMessage<TransferFulfillmentMessage>> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }
    
    //----------------------------------------------------
	//--DeadLetter  Consumer
    @Bean
    public ConsumerFactory<String, String> deadLetterConsumerFactory() throws Exception {
    	
        return new DefaultKafkaConsumerFactory<String, String>(listenerConfig, new StringDeserializer(),  new StringDeserializer());
    }
    
    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> deadLetterListenerContainerFactory() throws Exception {
    
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(deadLetterConsumerFactory());
        return factory;
    }

    //----------------------------------------------------
	//--CancelReservationFulfillmentResponse Message Consumer
    @Bean
    public ConsumerFactory<String, TraceableMessage<ResponseMessage<CancelReservationRequest,CancelReservationResponse>>>
    	cancelReservationConsumerFactory() throws Exception {
    	
    	JacksonAvroDeserializer<TraceableMessage<ResponseMessage<CancelReservationRequest,CancelReservationResponse>>> jad = new JacksonAvroDeserializer<>();
    	jad.configure(listenerConfig);
    	
        return new DefaultKafkaConsumerFactory<String, TraceableMessage<ResponseMessage<CancelReservationRequest,CancelReservationResponse>>>
        	(listenerConfig, new StringDeserializer(),  jad);
    }
    
    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, TraceableMessage<ResponseMessage<CancelReservationRequest,CancelReservationResponse>>>> 
    	cancelReservationListenerContainerFactory() throws Exception {
    
        ConcurrentKafkaListenerContainerFactory<String, TraceableMessage<ResponseMessage<CancelReservationRequest,CancelReservationResponse>>> 
        	factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(cancelReservationConsumerFactory());
        return factory;
    }

    //----------------------------------------------------
	//--CommitReservationFulfillmentResponse Message Consumer
    @Bean
    public ConsumerFactory<String, TraceableMessage<ResponseMessage<CommitReservationRequest,CommitReservationResponse>>>
    	commitReservationConsumerFactory() throws Exception {
    	
    	JacksonAvroDeserializer<TraceableMessage<ResponseMessage<CommitReservationRequest,CommitReservationResponse>>> jad
    				= new JacksonAvroDeserializer<>();
    	jad.configure(listenerConfig);
    	
        return new DefaultKafkaConsumerFactory<String, TraceableMessage<ResponseMessage<CommitReservationRequest,CommitReservationResponse>>>
        	(listenerConfig, new StringDeserializer(),  jad);
    }
    
    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, TraceableMessage<ResponseMessage<CommitReservationRequest,CommitReservationResponse>>>> 
    	commitReservationListenerContainerFactory() throws Exception {
    
        ConcurrentKafkaListenerContainerFactory<String, TraceableMessage<ResponseMessage<CommitReservationRequest,CommitReservationResponse>>>
        	factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(commitReservationConsumerFactory());
        return factory;
    }

    //----------------------------------------------------
	//--TransactionFulfillmentResponse Message Consumer
    @Bean
    public ConsumerFactory<String, TraceableMessage<ResponseMessage<CommitReservationRequest,CommitReservationResponse>>>
    	transactionConsumerFactory() throws Exception {
    	
    	JacksonAvroDeserializer<TraceableMessage<ResponseMessage<CommitReservationRequest,CommitReservationResponse>>> jad 
    		= new JacksonAvroDeserializer<>();
    	jad.configure(listenerConfig);
    	
        return new DefaultKafkaConsumerFactory<String, TraceableMessage<ResponseMessage<CommitReservationRequest,CommitReservationResponse>>>
        	(listenerConfig, new StringDeserializer(),  jad);
    }
    
    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, TraceableMessage<ResponseMessage<CommitReservationRequest,CommitReservationResponse>>>>
    	transactionListenerContainerFactory() throws Exception {
    
        ConcurrentKafkaListenerContainerFactory<String, TraceableMessage<ResponseMessage<CommitReservationRequest,CommitReservationResponse>>>
        	factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(transactionConsumerFactory());
        return factory;
    }

}
