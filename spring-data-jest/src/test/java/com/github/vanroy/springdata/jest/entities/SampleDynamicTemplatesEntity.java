package com.github.vanroy.springdata.jest.entities;

import java.util.HashMap;
import java.util.Map;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.DynamicTemplates;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

@Document(indexName = "test-dynamictemplates", type = "test-dynamictemplatestype", indexStoreType = "memory")
@DynamicTemplates(mappingPath = "/mappings/test-dynamic_templates_mappings.json")
public class SampleDynamicTemplatesEntity {

	@Id
	private String id;

	@Field(type = FieldType.Object)
	private Map<String, String> names = new HashMap<>();
}