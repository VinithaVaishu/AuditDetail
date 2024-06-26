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
//	.queryParam("table", "RefZonePivot")
//	  .queryParam("schema","dbo")
//	  .queryParam("lsn","0x0000031B000019AD008E")
	 Long start = System.currentTimeMillis();
int pageNo=1;
int noOfRowsChanged=0;
int TotalNoOfRows=table.getNumberOfRows();
	do
	{	 int page =pageNo;
	  List<Object> changedrows = client.get().uri(uriBuilder ->
	  uriBuilder.path("/Services/api/analytics/audit-data")
	  .queryParam("response_format", "JSON")
	  .queryParam("table", table.getChangedTableName())
	  .queryParam("schema",table.getSchemaName())
	  .queryParam("lsn",table.getLsn())
	  .queryParam("page",page)
	  .queryParam("page_size", 100).build())
	  .retrieve().bodyToFlux(Object.class).collectList().block();
	  kafkaProducerService.publishAuditData(changedrows, table,page);
	  TotalNoOfRows=TotalNoOfRows-100;
	  pageNo++;
	}while(TotalNoOfRows>0);
	 
	 
	  Long end = System.currentTimeMillis();
	  System.out.println( end-start);
	
	  }}	


