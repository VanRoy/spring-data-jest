package com.github.vanroy.springdata.jest.entities;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.*;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(indexName = "test-index-book-core-template", type = "book", shards = 1, replicas = 0,
		refreshInterval = "-1")
public class Book {

	@Id private String id;
	private String name;
	@Field(type = FieldType.Object) private Author author;
	@Field(type = FieldType.Nested) private Map<Integer, Collection<String>> buckets = new HashMap<>();
	@MultiField(mainField = @Field(type = FieldType.Text, analyzer = "whitespace"),
			otherFields = {@InnerField(suffix = "prefix", type = FieldType.Text, analyzer = "stop",
					searchAnalyzer = "standard")}) private String description;

	@Data
	private static class Author {

		private String id;
		private String name;
	}
}
