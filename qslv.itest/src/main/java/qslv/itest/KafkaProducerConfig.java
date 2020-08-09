package qslv.itest;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import com.fasterxml.jackson.databind.JavaType;

import qslv.common.kafka.JacksonAvroSerializer;
import qslv.common.kafka.TraceableMessage;
import qslv.transaction.request.CancelReservationRequest;
import qslv.transaction.request.CommitReservationRequest;
import qslv.transaction.request.TransactionRequest;
import qslv.transfer.request.TransferFulfillmentMessage;

@Configuration
@DependsOn({"kafkaPropertiesConfig"})
public class KafkaProducerConfig {
	private static final Logger log = LoggerFactory.getLogger(KafkaProducerConfig.class);

	@Autowired
	ConfigProperties configProperties;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Bean
	public Map<String,Object> producerConfig() throws Exception {
		Properties kafkaconfig = new Properties();
		try {
			kafkaconfig.load(new FileInputStream(configProperties.getKafkaProducerPropertiesPath()));
		} catch (Exception fileEx) {
			try {
				kafkaconfig.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(configProperties.getKafkaProducerPropertiesPath()));
			} catch (Exception resourceEx) {
				log.error("{} not found.", configProperties.getKafkaProducerPropertiesPath());
				log.error("File Exception. {}", fileEx.toString());
				log.error("Resource Exception. {}", resourceEx.toString());
				throw resourceEx;
			}
		}
		return new HashMap(kafkaconfig);
	}
	
	// Transfer Fulfillment
	@Bean
	public ProducerFactory<String, TraceableMessage<TransferFulfillmentMessage>> transferFulfillmentProducerFactory() throws Exception {
		JacksonAvroSerializer<TraceableMessage<TransferFulfillmentMessage>> jas = new JacksonAvroSerializer<>();
		JavaType type = jas.getTypeFactory().constructParametricType(TraceableMessage.class, TransferFulfillmentMessage.class);
		jas.configure(producerConfig(), false, type);
		return new DefaultKafkaProducerFactory<String, TraceableMessage<TransferFulfillmentMessage>>(producerConfig(),
				new StringSerializer(), jas);
	}

	@Bean
	public KafkaTemplate<String, TraceableMessage<TransferFulfillmentMessage>> transferFulfillmentKafkaTemplate() throws Exception {
		return new KafkaTemplate<>(transferFulfillmentProducerFactory(), true); // auto-flush true, to force each message to broker.
	}	
	
	// Cancel Fulfillment
	@Bean
	public ProducerFactory<String, TraceableMessage<CancelReservationRequest>> cancelFulfillmentProducerFactory() throws Exception {
		JacksonAvroSerializer<TraceableMessage<CancelReservationRequest>> jas = new JacksonAvroSerializer<>();
		JavaType type = jas.getTypeFactory().constructParametricType(TraceableMessage.class, CancelReservationRequest.class);
		jas.configure(producerConfig(), false, type);
		return new DefaultKafkaProducerFactory<String, TraceableMessage<CancelReservationRequest>>(producerConfig(),
				new StringSerializer(), jas);
	}

	@Bean
	public KafkaTemplate<String, TraceableMessage<CancelReservationRequest>> cancelFulfillmentKafkaTemplate() throws Exception {
		return new KafkaTemplate<>(cancelFulfillmentProducerFactory(), true); // auto-flush true, to force each message to broker.
	}	
	
	// Commit Fulfillment
	@Bean
	public ProducerFactory<String, TraceableMessage<CommitReservationRequest>> commitFulfillmentProducerFactory() throws Exception {
		JacksonAvroSerializer<TraceableMessage<CommitReservationRequest>> jas = new JacksonAvroSerializer<>();
		JavaType type = jas.getTypeFactory().constructParametricType(TraceableMessage.class, CommitReservationRequest.class);
		jas.configure(producerConfig(), false, type);
		return new DefaultKafkaProducerFactory<String, TraceableMessage<CommitReservationRequest>>(producerConfig(),
				new StringSerializer(), jas);
	}

	@Bean
	public KafkaTemplate<String, TraceableMessage<CommitReservationRequest>> commitFulfillmentKafkaTemplate() throws Exception {
		return new KafkaTemplate<>(commitFulfillmentProducerFactory(), true); // auto-flush true, to force each message to broker.
	}	
	
	// Transaction Fulfillment
	@Bean
	public ProducerFactory<String, TraceableMessage<TransactionRequest>> transactionFulfillmentProducerFactory() throws Exception {
		JacksonAvroSerializer<TraceableMessage<TransactionRequest>> jas = new JacksonAvroSerializer<>();
		JavaType type = jas.getTypeFactory().constructParametricType(TraceableMessage.class, TransactionRequest.class);
		jas.configure(producerConfig(), false, type);
		return new DefaultKafkaProducerFactory<String, TraceableMessage<TransactionRequest>>(producerConfig(),
				new StringSerializer(), jas);
	}

	@Bean
	public KafkaTemplate<String, TraceableMessage<TransactionRequest>> transactionFulfillmentKafkaTemplate() throws Exception {
		return new KafkaTemplate<>(transactionFulfillmentProducerFactory(), true); // auto-flush true, to force each message to broker.
	}
}
