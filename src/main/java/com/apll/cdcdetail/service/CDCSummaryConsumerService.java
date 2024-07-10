package com.apll.cdcdetail.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import com.apll.cdcdetail.model.CDCSummaryResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
@Service
public class CDCSummaryConsumerService {

	@Autowired
	public CDCDetailDataFetchService DetailFetch;

	@KafkaListener(groupId = "summary_consumer_group", topics = "com.apll.cargowise.summary")
	public void readMessage(ConsumerRecord<String, String> message, Acknowledgment acknowledgment) {

		ObjectMapper mapper = new ObjectMapper();
		System.out.println("PARTITION: " + message.partition() + "_OFFSET: " + message.offset());

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

	}
}