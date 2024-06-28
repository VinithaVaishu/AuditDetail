package com.apll.cdcdetail.util;

import org.springframework.stereotype.Component;

import com.apll.cdcdetail.constants.AppConstants;
import com.fasterxml.jackson.databind.JsonNode;

@Component
public class AppUtil {
	
	
	public static String getTopic(String key) {
		String topic = null;
		 int val1= (key.toString().hashCode()) %30+ 1;
			val1 = Math.abs(val1);
			if(val1 > 0 && val1 <= 6) {
				topic = AppConstants.producerKafkaTopic_1;
			} else if (val1 > 6 && val1 <= 12) {
				topic = AppConstants.producerKafkaTopic_2;
			} else if (val1 > 12 && val1 <= 18) {
				topic = AppConstants.producerKafkaTopic_3;
			} else if (val1 > 18 && val1 <= 24) {
				topic = AppConstants.producerKafkaTopic_4;
			}else{
				topic = AppConstants.producerKafkaTopic_5;
			}
		return topic;
	}

}
