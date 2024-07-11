package com.apll.cdcdetail.service;

import java.io.IOException;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.WebClient;

import com.apll.cdcdetail.constants.AppConstants;
import com.apll.cdcdetail.model.CDCSummaryResponse;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

@Service
@Slf4j
public class CDCSummaryConsumerService {

	@Autowired
	public CDCDetailDataFetchService DetailFetch;

	@RetryableTopic(attempts = "2", backoff = @Backoff(delay = 1000, multiplier = 2, maxDelay = 10000))
	@KafkaListener(groupId = "summary_consumer_group", topics = "cdr-summary-topic-01")
	public void readMessage(ConsumerRecord<String, String> message, Acknowledgment acknowledgment) {
		log.info("Message consumed from topic " + AppConstants.PARTITION + message.partition() + AppConstants.OFFSET
				+ message.offset());
		ObjectMapper mapper = new ObjectMapper();
		try {
			CDCSummaryResponse summaryRecord = mapper.readValue(message.value(), CDCSummaryResponse.class);
			DetailFetch.detailApiCall(summaryRecord);

			log.info("Processing successful. Acknowledging the message.");
			acknowledgment.acknowledge();
			log.info("Message acknowledged successfully.");
		} catch (JsonProcessingException e) {
			log.info("Json processing Exception occred" + e.getMessage());
			e.printStackTrace();
		}

	}

	@DltHandler
	public void dltHandler(ConsumerRecord<String, String> message, Acknowledgment acknowledgment) {
		log.info("Message consumed from  Dead letter topic." + AppConstants.PARTITION + message.partition()
				+ AppConstants.OFFSET + message.offset());
		ObjectMapper mapper = new ObjectMapper();
		try {
			CDCSummaryResponse summaryRecord = mapper.readValue(message.value(), CDCSummaryResponse.class);
			DetailFetch.detailApiCall(summaryRecord);

			log.info("Processing successful. Acknowledging the message.");
			acknowledgment.acknowledge();
			log.info("Message acknowledged successfully.");
		} catch (JsonProcessingException e) {
			log.info("Json processing Exception occred" + e.getMessage());
			e.printStackTrace();
		}

	}
}