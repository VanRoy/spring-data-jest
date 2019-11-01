package com.github.vanroy.springdata.jest.entities;

import org.springframework.data.elasticsearch.annotations.Document;

@Document(indexName = "test-index-2", replicas = 0, shards = 1)
public class ResultAggregator {

	private final String id;
	private final String firstName;
	private final String lastName;

	public ResultAggregator(String id, String firstName, String lastName) {
		this.id = id;
		this.firstName = firstName;
		this.lastName = lastName;
	}
}