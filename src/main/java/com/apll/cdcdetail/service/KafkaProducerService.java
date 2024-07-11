package com.apll.cdcdetail.service;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import com.apll.cdcdetail.model.CDCSummaryResponse;
import com.apll.cdcdetail.util.AppUtil;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class KafkaProducerService {

	@Autowired
	private KafkaTemplate<String, String> template;

	public void publishAuditData(List<Object> DetailRecordsList, CDCSummaryResponse table, int page) {

		log.info("publishing of detail records to detail kafka topic started");
		DetailRecordsList.stream().map(detailRecord -> publishBasedOnPK(detailRecord)).collect(Collectors.toList());
		log.info("publishing of detail records to detail kafka topic ended");
	}

	private String publish(Object detailRecord, CDCSummaryResponse table, int page) {
		String key = table.getChangedTableName() + "_" + table.getLsn() + "_" + page;
		// String key = table.getChangedTableName()+"_"+table.getLsn();
		System.out.println(key);
		String topic = AppUtil.getTopic(key);
		CompletableFuture<SendResult<String, String>> result = template.send("cdr-detail-topic-01", key,
				detailRecord.toString());

		return result.toString();
	}

	private String publishBasedOnPK(Object detailrecord) {

		//System.out.println(key + "_" + JsonformatDetailRecord);
		CompletableFuture<SendResult<String, String>> future = template.send("cdr-detail-topic-01", AppUtil.getKey(detailrecord.toString()),
				AppUtil.getJsonFormatString(detailrecord.toString()));
		future.whenComplete((result,ex)->{
			ProducerRecord<String, String> producerRecord= result.getProducerRecord();
			RecordMetadata  metadata =result.getRecordMetadata();
			if(ex==null) {
				log.info("Message succefully published.");
				log.info("Topic:"+metadata.topic() + "_Partition:" + metadata.partition()+ "_Offset:"+metadata.offset());
				log.debug(producerRecord.value());
			}
			else {
				log.info("Message failed to publish. Key:"+ producerRecord.key()+" _message:"+producerRecord.value());
				log.error(ex.getMessage() +"_"+ex.getCause());
			}
		});
		return future.toString();
	}
}
