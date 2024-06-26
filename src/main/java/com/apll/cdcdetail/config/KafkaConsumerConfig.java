package com.apll.cdcdetail.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import com.apll.cdcdetail.model.CDCSummaryResponse;

@Configuration
@EnableKafka
public class KafkaConsumerConfig {
	 @Value("${spring.kafka.consumer.bootstrap-servers}")
	    private String bootstrapServers;
	 
	 @Value("${spring.kafka.consumer.group-id}")
	    private String groupId;
	@Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap();
        props.put(
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, 
          bootstrapServers);
        props.put(
          ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
          StringDeserializer.class);
        props.put(
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
          JsonDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "summary_consumer_group");
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); 
       
       return props;
    }

	 @Bean
	    public ConsumerFactory<String, CDCSummaryResponse> consumerFactory() {
	        return new DefaultKafkaConsumerFactory<>(consumerConfigs(), new StringDeserializer(),
	                new JsonDeserializer<>(CDCSummaryResponse.class));
	    }
   
    
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, CDCSummaryResponse> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, CDCSummaryResponse> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE); // manual acknowledgment mode

        return factory;
    }
}

