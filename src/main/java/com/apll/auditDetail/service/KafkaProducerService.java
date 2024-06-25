package com.apll.auditDetail.service;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import com.apll.auditDetail.config.KafkaProducerConfig;
import com.apll.auditDetail.model.ChangedTable;

@Service
public class KafkaProducerService {
	
	@Autowired
	private KafkaTemplate<String, String>  template; 
	
	public void publishAuditData(List<Object> changedRows, ChangedTable table, int page) {
		
	System.out.println(table.toString());
	//publish(changedRows.get(0),table,page);

			changedRows.stream()
			.map(row -> publish(row,table,page))
				.forEach(t->System.out.println(t));
	}

	private String publish(Object row, ChangedTable table,int page) {
		String key = table.getChangedTableName()+"_"+table.getLsn()+"_"+page;
		System.out.println(key);
		CompletableFuture<SendResult<String, String>> result = template.send("com.apll.cargowise.detail_1",key, row.toString());
		
		return result.toString();
	}

}
