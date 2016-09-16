/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.vanroy.springdata.jest.entities;

import static org.springframework.data.elasticsearch.annotations.FieldIndex.*;
import static org.springframework.data.elasticsearch.annotations.FieldType.String;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;

/**
 * @author Rizwan Idrees
 * @author Mohsin Husen
 */
@Data
@Document(indexName = "test-mapping", type = "mapping", shards = 1, replicas = 0, refreshInterval = "-1")
public class SampleMappingEntity {

	@Id
	private String id;

	@Field(type = String, index = not_analyzed, store = true, analyzer = "standard")
	private String message;

	private NestedEntity nested;

	@Data
	static class NestedEntity {

		@Field(type = String)
		private String someField;

	}
}
