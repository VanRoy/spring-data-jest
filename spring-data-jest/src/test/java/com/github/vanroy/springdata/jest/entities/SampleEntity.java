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

import static org.springframework.data.elasticsearch.annotations.FieldType.Text;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.ScriptedField;
import org.springframework.data.elasticsearch.core.geo.GeoPoint;

/**
 * @author Rizwan Idrees
 * @author Mohsin Husen
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Document(indexName = "test-index", type = "test-type", shards = 1, replicas = 0, refreshInterval = "-1")
public class SampleEntity {

	@Id
	private String id;
	@Field(type = Text, store = true, fielddata = true)
	private String type;
	@Field(type = Text, store = true, fielddata = true)
	private String message;
	private int rate;
	@ScriptedField
	private Long scriptedRate;
	private boolean available;
	private String highlightedMessage;

	private GeoPoint location;

	@Version
	private Long version;
}
