package com.github.vanroy.springboot.autoconfigure.data.jest;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.util.Arrays;

import com.github.vanroy.springboot.autoconfigure.data.jest.entities.Product;
import com.github.vanroy.springboot.autoconfigure.data.jest.repositories.ProductRepository;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.elasticsearch.ElasticsearchAutoConfiguration;
import org.springframework.boot.autoconfigure.data.elasticsearch.ElasticsearchDataAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = ElasticsearchJestAutoConfigurationTest.SpringBootStarterDataJestApplication.class)
public class ElasticsearchJestAutoConfigurationTest {

	@Autowired
	private ProductRepository repository;

	@Autowired
	private ElasticsearchOperations elasticsearchOperations;

	@Before
	public void before() {
		elasticsearchOperations.deleteIndex(Product.class);
		elasticsearchOperations.createIndex(Product.class);
		elasticsearchOperations.putMapping(Product.class);
		elasticsearchOperations.refresh(Product.class);

		repository.save(Arrays.asList(
				Product.builder().id("1").name("Sugar").text("Cane sugar").price(1.0f).available(false).build()
				, Product.builder().id("2").name("Sugar").text("Cane sugar").price(1.2f).available(true).build()
				, Product.builder().id("3").name("Sugar").text("Beet sugar").price(1.1f).available(true).build()
				, Product.builder().id("4").name("Salt").text("Rock salt").price(1.9f).available(true).build()
				, Product.builder().id("5").name("Salt").text("Sea salt").price(2.1f).available(false).build()));

		elasticsearchOperations.refresh(Product.class);
	}

	@Test
	public void should_repository_support_find() {
		assertThat(repository.findByNameAndText("Sugar", "Cane sugar").size(), is(2));
		assertThat(repository.findByNameAndPrice("Sugar", 1.1f).size(), is(1));
	}

	@SpringBootApplication(exclude = {
		ElasticsearchAutoConfiguration.class,
		ElasticsearchDataAutoConfiguration.class,
		ElasticsearchJestAWSAutoConfiguration.class
	},
	scanBasePackageClasses = ProductRepository.class)
	public static class SpringBootStarterDataJestApplication {
	}
}
