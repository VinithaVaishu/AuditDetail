package com.apll.cdcdetail.config;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.springframework.stereotype.Component;

@Component
public class CustomPartitioner  implements Partitioner {



	@Override
	public void configure(Map<String, ?> configs) {
		// TODO Auto-generated method stub

	}

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		// Custom logic to determine partition based on the key
		// You can use any logic you want to determine the partition
		// For example, you can hash the key and then mod it with the number of
		// partitions
		Integer val1 = (key.toString().hashCode()) %30+ 1;
		val1 = Math.abs(val1);
		  
		int partition = val1%6 ;
		
		return partition;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

}
