package com.github.vanroy.springboot.autoconfigure.data.jest;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.test.util.TestPropertyValues;
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
	public void should_jest_auto_configuration_have_proxy() {
		load("spring.data.jest.uri=http://localhost:9200", "spring.data.jest.proxy.host=proxy.example.com",
				"spring.data.jest.proxy.port=8080");
	}

	@Test
	public void should_jest_auto_configuration_have_proxy_without_port() {
		this.thrown.expect(BeanCreationException.class);
		this.thrown.expectMessage("Proxy port must not be null");
		load("spring.data.jest.uri=http://localhost:9200", "spring.data.jest.proxy.host=proxy.example.com");
	}

	private void load(String... environment) {
		load(null, environment);
	}

	private void load(Class<?> config, String... environment) {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();

		TestPropertyValues.of(environment).applyTo(context);
		if (config != null) {
			context.register(config);
		}
		context.register(ElasticsearchJestAutoConfiguration.class);
		context.refresh();
		this.context = context;
	}
}
