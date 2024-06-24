package com.apll.auditDetail.service;

import java.io.IOException;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.WebClient;

import com.apll.auditDetail.model.ChangedTable;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import reactor.core.publisher.Mono;

@Service
public class KafkaConsumerService {
	@Autowired
	public CargowiseApiPollingService pollingServcie;
	
	@KafkaListener(topicPartitions 
			  = @TopicPartition(topic = "com.apll.cargowise.summary", partitions = { "0"}))
	public void readMessage(String message) {
		ObjectMapper mapper = new ObjectMapper();
		
			try {
				ChangedTable changedTable = mapper.readValue(message, ChangedTable.class);
				pollingServcie.detailApiPolling(changedTable);
			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
			
			System.out.println(message);
	}}