package com.apll.cdcdetail.constants;

import org.springframework.stereotype.Component;

@Component
public class AppConstants {
	public static final String producerKafkaTopic_1 = "com.apll.cargowise.detail_1";
	public static final String producerKafkaTopic_2 = "com.apll.cargowise.detail_2";
	public static final String producerKafkaTopic_3 = "com.apll.cargowise.detail_3";
	public static final String producerKafkaTopic_4 = "com.apll.cargowise.detail_4";
	public static final String producerKafkaTopic_5 = "com.apll.cargowise.detail_5";

	public static final int PARTITION_1 = 0;
	public static final int PARTITION_2 = 1;
	public static final int PARTITION_3 = 2;
	public static final int PARTITION_4 = 3;
	public static final int PARTITION_5 = 4;
	public static final int PARTITION_6 = 6;
	
	public static final String  page="page";
	public static final String  response_format="response_format";
	public static final String  schema="schema";
	public static final String  lsn="lsn";
	public static final String  table="table";
	public static final String  page_size="page_size";
	public static final String  PARTITION="PARTITION : ";
	public static final String  OFFSET="OFFSET : ";
	

}
