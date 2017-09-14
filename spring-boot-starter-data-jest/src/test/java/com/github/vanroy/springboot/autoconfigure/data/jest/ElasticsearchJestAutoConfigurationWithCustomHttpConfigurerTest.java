package com.github.vanroy.springboot.autoconfigure.data.jest;


import static org.hamcrest.core.Is.*;
import static org.junit.Assert.*;

import com.github.vanroy.springboot.autoconfigure.data.jest.repositories.ProductRepository;
import io.searchbox.client.JestClient;
import io.searchbox.client.http.JestHttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.Configurable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.elasticsearch.ElasticsearchAutoConfiguration;
import org.springframework.boot.autoconfigure.data.elasticsearch.ElasticsearchDataAutoConfiguration;
import org.springframework.boot.autoconfigure.elasticsearch.jest.HttpClientConfigBuilderCustomizer;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = ElasticsearchJestAutoConfigurationWithCustomHttpConfigurerTest.SpringBootStarterDataJestApplication.class)
public class ElasticsearchJestAutoConfigurationWithCustomHttpConfigurerTest {

	@Autowired
	private JestClient jestClient;

	@Test
	public void should_jest_auto_configuration_have_custom_httpbuilder() {
		RequestConfig config = ((Configurable) ((JestHttpClient) jestClient).getHttpClient()).getConfig();

		int connectTimeout = config.getConnectTimeout();

		assertThat(connectTimeout, is(3551));
	}

	@SpringBootApplication(exclude = {
			ElasticsearchAutoConfiguration.class,
			ElasticsearchDataAutoConfiguration.class,
			ElasticsearchJestAWSAutoConfiguration.class
	},
			scanBasePackageClasses = ProductRepository.class)
	public static class SpringBootStarterDataJestApplication {

		@Bean
		public HttpClientConfigBuilderCustomizer customizer() {
			return httpBuilder ->
					httpBuilder
							.connTimeout(3551);
		}
	}
}
