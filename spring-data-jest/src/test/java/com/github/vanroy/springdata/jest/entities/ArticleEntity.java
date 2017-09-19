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

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.InnerField;
import org.springframework.data.elasticsearch.annotations.MultiField;

import java.util.ArrayList;
import java.util.List;

import static org.springframework.data.elasticsearch.annotations.FieldType.Integer;
import static org.springframework.data.elasticsearch.annotations.FieldType.text;

/**
 * Simple type to test facets
 *
 * @author Artur Konczak
 * @author Mohsin Husen
 */
@Document(indexName = "articles", type = "article", shards = 1, replicas = 0, refreshInterval = "-1")
public class ArticleEntity {

	@Id
	private String id;
	private String title;
	@Field(type = text, fielddata = true)
	private String subject;

	@MultiField(
			mainField = @Field(type = text, index = false, store = true),
			otherFields = {
					@InnerField(suffix = "untouched", type = text, store = true, index = false),
					@InnerField(suffix = "sort", type = text, store = true, indexAnalyzer = "keyword")
			}
	)
	private List<String> authors = new ArrayList<>();

	@Field(type = Integer, store = true)
	private List<Integer> publishedYears = new ArrayList<>();

	private int score;

	private ArticleEntity() {

	}

	public ArticleEntity(String id) {
		this.id = id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getId() {
		return id;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getSubject() {
		return subject;
	}

	public void setSubject(String subject) {
		this.subject = subject;
	}

	public List<String> getAuthors() {
		return authors;
	}

	public void setAuthors(List<String> authors) {
		this.authors = authors;
	}

	public List<Integer> getPublishedYears() {
		return publishedYears;
	}

	public void setPublishedYears(List<Integer> publishedYears) {
		this.publishedYears = publishedYears;
	}

	public int getScore() {
		return score;
	}

	public void setScore(int score) {
		this.score = score;
	}
}
