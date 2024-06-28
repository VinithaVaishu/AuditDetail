package com.apll.cdcdetail.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.WebClient;

import com.apll.cdcdetail.constants.AppConstants;
import com.apll.cdcdetail.model.CDCSummaryResponse;
import com.fasterxml.jackson.databind.JsonNode;

import jakarta.annotation.PostConstruct;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class CDCDetailDataFetchService {
	@Autowired
	public KafkaProducerService kafkaProducerService;
	@Value("${cdc.detail.api.username}")
	private String username;
	@Value("${cdc.detail.api.password}")
	private String password;
	@Value("${cdc.detail.api.base_url}")
	private String baseurl;
	@Value("${cdc.detail.api.path}")
	private String path;
	@Value("${cdc.detail.api.response_format}")
	private String responseFormat;
	@Value("${cdc.detail.api.page_size}")
	private String pageSize;
	
	public void detailApiCall(CDCSummaryResponse table) {
		ExchangeFilterFunction basicAuthenticationFilter = ExchangeFilterFunction.ofRequestProcessor(clientRequest -> {
			ClientRequest newRequest = ClientRequest.from(clientRequest).headers(headers -> {
				headers.setBasicAuth(username, password);
			}).build();
			return Mono.just(newRequest);
		});

		WebClient client = WebClient.builder().baseUrl(baseurl) 
				.filter(basicAuthenticationFilter).build();
		Long start = System.currentTimeMillis();
		int pageNo = 1;
		int noOfRowsChanged = 0;
		int TotalNoOfRows = table.getNumberOfRows();
		do {
			int page = pageNo;
			List<Object> changedrows = client.get()
					.uri(uriBuilder -> uriBuilder.path(path)
							.queryParam(AppConstants.response_format, responseFormat).queryParam(AppConstants.table, table.getChangedTableName())
							.queryParam(AppConstants.schema, table.getSchemaName()).queryParam(AppConstants.lsn, table.getLsn())
							.queryParam(AppConstants.page, page).queryParam(AppConstants.page_size, pageSize).build())
					.retrieve().bodyToFlux(Object.class).collectList().block();
			kafkaProducerService.publishAuditData(changedrows, table, page);
			TotalNoOfRows = TotalNoOfRows - 100;
			pageNo++;
		} while (TotalNoOfRows > 0);

		Long end = System.currentTimeMillis();
		System.out.println(end - start);

	}
}
