package com.github.vanroy.springboot.autoconfigure.data.jest;

import org.elasticsearch.node.NodeValidationException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.test.util.EnvironmentTestUtils;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
public class ElasticsearchJestAutoConfigurationWithProxyTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	protected AnnotationConfigApplicationContext context;

	@Before
	public void setupMock() {
		MockitoAnnotations.initMocks(this);

	}

	@Test
	public void should_jest_auto_configuration_have_proxy() throws NodeValidationException {
		load("spring.data.jest.uri=http://localhost:9200", "spring.data.jest.proxy.host=proxy.example.com",
				"spring.data.jest.proxy.port=8080");

	}

	@Test
	public void should_jest_auto_configuration_have_proxy_without_port() throws NodeValidationException {
		this.thrown.expect(BeanCreationException.class);
		this.thrown.expectMessage("Proxy port must not be null");
		load("spring.data.jest.uri=http://localhost:9200", "spring.data.jest.proxy.host=proxy.example.com");
	}

	private void load(String... environment) {
		load(null, environment);
	}

	@SuppressWarnings("deprecation")
	private void load(Class<?> config, String... environment) {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		EnvironmentTestUtils.addEnvironment(context, environment);
		if (config != null) {
			context.register(config);
		}
		context.register(ElasticsearchJestAutoConfiguration.class);
		context.refresh();
		this.context = context;
	}

}
