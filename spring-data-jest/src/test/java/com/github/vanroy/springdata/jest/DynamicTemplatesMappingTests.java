package com.github.vanroy.springdata.jest;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.github.vanroy.springdata.jest.entities.SampleDynamicTemplatesEntity;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.Test;

public class DynamicTemplatesMappingTests {

	@Test
	public void testCorrectDynamicTemplatesMappings() throws IOException {
		XContentBuilder xContentBuilder = MappingBuilder.buildMapping(SampleDynamicTemplatesEntity.class,
				"test-dynamictemplatestype", "id", null);
		String EXPECTED_MAPPING_ONE = "{\"test-dynamictemplatestype\":{\"dynamic_templates\":" +
				"[{\"with_custom_analyzer\":{" +
				"\"mapping\":{\"type\":\"string\",\"analyzer\":\"standard_lowercase_asciifolding\"}," +
				"\"path_match\":\"names.*\"}}]," +
				"\"properties\":{\"names\":{\"type\":\"object\"}}}}";
		assertEquals(EXPECTED_MAPPING_ONE, xContentBuilderToString(xContentBuilder));
	}

	private String xContentBuilderToString(XContentBuilder builder) {
		builder.close();
		ByteArrayOutputStream bos = (ByteArrayOutputStream) builder.getOutputStream();
		return bos.toString();
	}
}
