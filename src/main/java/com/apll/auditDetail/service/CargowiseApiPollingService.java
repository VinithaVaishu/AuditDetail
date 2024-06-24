package com.apll.auditDetail.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.WebClient;

import com.apll.auditDetail.model.ChangedTable;
import com.fasterxml.jackson.databind.JsonNode;

import jakarta.annotation.PostConstruct;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class CargowiseApiPollingService {
	@Autowired
	public KafkaProducerService kafkaProducerService;
	
	public void detailApiPolling(ChangedTable table){
	ExchangeFilterFunction basicAuthenticationFilter = ExchangeFilterFunction.ofRequestProcessor(clientRequest -> {
		ClientRequest newRequest = ClientRequest.from(clientRequest).headers(headers -> {
			headers.add("Cookie", "WEBSVC=58b7de25b1e11abb");
			headers.setBasicAuth("A0P.Audit.API.APLL", "AHPeD5rp8eN2Xj9Eqq*3rH");
		}).build();
		return Mono.just(newRequest);
	});

	

	WebClient client = WebClient.builder().baseUrl("https://svc-a0ptrn.wisegrid.net")
			.filter(basicAuthenticationFilter).build();

	
	
	  List<Object> changedrows = client.get().uri(uriBuilder ->
	  uriBuilder.path("/Services/api/analytics/audit-data")
	  .queryParam("response_format", "JSON")
	  .queryParam("table", table.getChangedTableName())
	  .queryParam("schema",table.getSchemaName())
	  .queryParam("lsn",table.getLsn())
//	  .queryParam("table", "RefZonePivot")
//	  .queryParam("schema","dbo")
//	  .queryParam("lsn","0x0000031B000019AD008E")
	  .queryParam("page",1)
	  .queryParam("page_size", 10000).build())
	  .retrieve().bodyToFlux(Object.class).collectList().block();
	  
	  Long start = System.currentTimeMillis();
	  for (Object object : changedrows) {
		kafkaProducerService.publishAuditData(changedrows, table);
	}
     
	  Long end = System.currentTimeMillis();
	  System.out.println( end-start);
	
	  }}	


