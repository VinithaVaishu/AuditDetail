package com.apll.cdcdetail.config;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

@Configuration
public class KafkaProducerConfig {
	@Value("${spring.kafka.producer.bootstrap-servers}")
    private String bootstrapServers;
	@Value("${spring.kafka.properties.security.protocol}")
    private String securityProtocol;
	@Value("${spring.kafka.properties.sasl.mechanism}")
    private String saslMechanism;
	@Value("${spring.kafka.properties.sasl.jaas.config}")
    private String saslJaasConfig;
	 
	

    @Bean
    public ProducerFactory<String, String> producerFactory() {
    	  var configProps = new java.util.HashMap<String, Object>();
          configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
          configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
          configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, RawJsonStringSerializer.class);
          configProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class);
          configProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
          configProps.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
          configProps.put(SaslConfigs.SASL_JAAS_CONFIG,"org.apache.kafka.common.security.plain.PlainLoginModule required username='TZNHIDR3TZFBPDYO' password='Jqx/UVRzg2yTpAYh/yLF3iRk7Fejep8dzWkPwzliCRrBLHQReAHqstt9j2VF8m/u';");
          
        return new DefaultKafkaProducerFactory<>(configProps);
    }
    
  
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
    
    


}
