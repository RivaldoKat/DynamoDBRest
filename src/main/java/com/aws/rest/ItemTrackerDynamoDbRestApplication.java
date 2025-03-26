package com.aws.rest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ItemTrackerDynamoDbRestApplication{

	public static void main(String[] args) {
		SpringApplication.run(ItemTrackerDynamoDbRestApplication.class, args);
	}

}
