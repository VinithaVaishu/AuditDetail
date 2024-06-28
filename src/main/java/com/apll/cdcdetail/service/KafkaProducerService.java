package com.apll.cdcdetail.service;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import com.apll.cdcdetail.model.CDCSummaryResponse;
import com.apll.cdcdetail.util.AppUtil;

@Service
public class KafkaProducerService {

	@Autowired
	private KafkaTemplate<String, String> template;

	public void publishAuditData(List<Object> DetailRecordsList, CDCSummaryResponse table, int page) {

		System.out.println(table.toString());  
 

			DetailRecordsList.stream()
			.map(detailRecord -> publishBasedOnPK(detailRecord))
				.collect(Collectors.toList());
	}

	private String publish(Object detailRecord, CDCSummaryResponse table, int page) {
		String key = table.getChangedTableName() + "_" + table.getLsn() + "_" + page;
		// String key = table.getChangedTableName()+"_"+table.getLsn();
		System.out.println(key);
		String topic= AppUtil.getTopic(key);
		CompletableFuture<SendResult<String, String>> result = template.send(topic, key,
				detailRecord.toString());

		return result.toString();
	}

	private String publishBasedOnPK(Object detailrecord) {
		String key = null;
		String topic=null;
		String[] fields = detailrecord.toString().split(",");
		for (int i = 0; i < fields.length; i++) {
			if (fields[i].contains("_PK=")) {
				key = fields[i].substring(7);
				break;
			}
		}
		topic= AppUtil.getTopic(key);
		
		System.out.println(topic+"_"+key+"_"+detailrecord.toString());
		CompletableFuture<SendResult<String, String>> result =template.send(topic, key,
				detailrecord.toString());
		
		
		return result.toString();
	}
}
