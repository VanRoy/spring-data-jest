/*
 * Copyright 2014 the original author or authors.
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

import lombok.Data;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.elasticsearch.annotations.Document;

/**
 * @author Abdul Waheed
 * @author Mohsin Husen
 */
@Data
@Document(indexName = "test-index-2", type = "hetro", replicas = 0, shards = 1)
public class HetroEntity2 {

	@Id
	private String id;
	private String lastName;
	@Version
	private Long version;

	public HetroEntity2(String id, String lastName) {
		this.id = id;
		this.lastName = lastName;
		this.version = System.currentTimeMillis();
	}
}
