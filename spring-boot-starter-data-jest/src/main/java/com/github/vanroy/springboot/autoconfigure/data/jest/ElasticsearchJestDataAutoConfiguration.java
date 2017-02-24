package com.github.vanroy.springboot.autoconfigure.data.jest;

import com.github.vanroy.springdata.jest.JestElasticsearchTemplate;
import com.github.vanroy.springdata.jest.mapper.DefaultJestResultsMapper;
import io.searchbox.client.JestClient;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.convert.ElasticsearchConverter;
import org.springframework.data.elasticsearch.core.convert.MappingElasticsearchConverter;
import org.springframework.data.elasticsearch.core.mapping.SimpleElasticsearchMappingContext;

/**
 * Created by julien on 23/06/2016.
 */
@Configuration
@AutoConfigureAfter(ElasticsearchJestAutoConfiguration.class)
public class ElasticsearchJestDataAutoConfiguration {

	@Bean
	@ConditionalOnMissingBean
	public ElasticsearchOperations elasticsearchTemplate(JestClient client) {
		return new JestElasticsearchTemplate(client);
	}

	@Bean
	@ConditionalOnMissingBean
	public ElasticsearchConverter elasticsearchConverter(SimpleElasticsearchMappingContext mappingContext) {
		return new MappingElasticsearchConverter(mappingContext);
	}

	@Bean
	@ConditionalOnMissingBean
	public SimpleElasticsearchMappingContext mappingContext() {
		return new SimpleElasticsearchMappingContext();
	}

}