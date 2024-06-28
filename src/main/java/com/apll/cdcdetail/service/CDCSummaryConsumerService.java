package com.apll.cdcdetail.service;

import java.io.IOException;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.WebClient;

import com.apll.cdcdetail.model.CDCSummaryResponse;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import reactor.core.publisher.Mono;

@Service
public class CDCSummaryConsumerService {  
	
	
	@Autowired
	public CDCDetailDataFetchService DetailFetch;
	
	@KafkaListener(groupId = "summary_consumer_group", topics = "com.apll.cargowise.summary")
	public void readMessage(ConsumerRecord<String,String> message, Acknowledgment acknowledgment) {
		ObjectMapper mapper = new ObjectMapper();
	     System.out.println("PARTITION: "+message.partition()+"_OFFSET: "+message.offset());
		
			try {
				CDCSummaryResponse summaryRecord = mapper.readValue(message.value(), CDCSummaryResponse.class);
				DetailFetch.detailApiCall(summaryRecord);
				
				System.out.println("Processing successful. Acknowledging the message.");
//				
				acknowledgment.acknowledge();
				
 
	            // Log after acknowledging
	            System.out.println("Message acknowledged successfully.");
			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
			
			
	}}