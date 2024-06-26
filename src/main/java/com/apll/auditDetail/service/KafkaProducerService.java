package com.apll.auditDetail.service;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import com.apll.auditDetail.model.ChangedTable;

@Service
public class KafkaProducerService {

	@Autowired
	private KafkaTemplate<String, String> template;

	public void publishAuditData(List<Object> changedRows, ChangedTable table, int page) {

		System.out.println(table.toString());


			changedRows.stream()
			.map(row -> publishBasedOnPK(row))
				.collect(Collectors.toList());
	}

	private String publish(Object row, ChangedTable table, int page) {
		String key = table.getChangedTableName() + "_" + table.getLsn() + "_" + page;
		// String key = table.getChangedTableName()+"_"+table.getLsn();
		System.out.println(key);
		CompletableFuture<SendResult<String, String>> result = template.send("com.apll.cargowise.detail_1", key,
				row.toString());

		return result.toString();
	}

	private String publishBasedOnPK(Object row) {
		String key = null;
		String[] fields = row.toString().split(",");
		for (int i = 0; i < fields.length; i++) {
			if (fields[i].contains("_PK=")) {
				key = fields[i].substring(7);
				
				break;
			}

		}
		System.out.println(key +"    :   "+row.toString());
		CompletableFuture<SendResult<String, String>> result = template.send("com.apll.cargowise.detail_2", key,
				row.toString());
		

		return result.toString();
	}
}
