package com.github.vanroy.springboot.autoconfigure.data.jest;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.google.common.base.Supplier;
import io.searchbox.client.JestClientFactory;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cloud.aws.core.region.RegionProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;
import vc.inreach.aws.request.AWSSigner;
import vc.inreach.aws.request.AWSSigningRequestInterceptor;

/**
 * Jest Elasticsearch for signing request on AWS configuration.
 * @author Julien Roy
 */
@Configuration
@ConditionalOnClass(AWSSigner.class)
@AutoConfigureAfter(name = "org.springframework.cloud.aws.autoconfigure.context.ContextRegionProviderAutoConfiguration")
public class ElasticsearchJestAWSAutoConfiguration {

	private static final String AWS_SERVICE = "es";
	private static final Supplier<LocalDateTime> CLOCK = () -> LocalDateTime.now(ZoneOffset.UTC);

	@Autowired
	private ElasticsearchJestProperties properties;

	@Autowired
	@Qualifier("elasticsearchJestAwsRegion")
	private String regionName;

	@Bean
	@ConditionalOnMissingBean(AWSCredentialsProvider.class)
	public AWSCredentialsProvider awsCredentialsProvider() {
		return new DefaultAWSCredentialsProviderChain();
	}

	@Bean
	public JestClientFactory jestClientFactory(AWSCredentialsProvider credentialsProvider) {

		final AWSSigner awsSigner = new AWSSigner(credentialsProvider, getRegion(), AWS_SERVICE, CLOCK);

		final AWSSigningRequestInterceptor requestInterceptor = new AWSSigningRequestInterceptor(awsSigner);
		return new JestClientFactory() {
			@Override
			protected HttpClientBuilder configureHttpClient(HttpClientBuilder builder) {
				builder.addInterceptorLast(requestInterceptor);
				return builder;
			}
			@Override
			protected HttpAsyncClientBuilder configureHttpClient(HttpAsyncClientBuilder builder) {
				builder.addInterceptorLast(requestInterceptor);
				return builder;
			}
		};
	}

	/**
	 * Return configured region if exist, else try to use auto-discovered region.
	 * @return Region name
	 */
	private String getRegion() {
		// Use specific user configuration
		if (StringUtils.hasText(properties.getAwsRegion())) {
			return properties.getAwsRegion();
		}

		return regionName;
	}

	@ConditionalOnMissingBean(name = "elasticsearchJestAwsRegion")
	@Bean(name = "elasticsearchJestAwsRegion")
	public String regionFromEC2() {

		// Try to determine current region ( work on EC2 instance )
		Region region = Regions.getCurrentRegion();
		if (region != null) {
			return region.getName();
		}

		// Nothing else , back to default
		return Regions.DEFAULT_REGION.getName();
	}

	@ConditionalOnClass(RegionProvider.class)
	@ConditionalOnBean(RegionProvider.class)
	private final static class RegionFromSpringCloudConfiguration {

		@Bean(name = "elasticsearchJestAwsRegion")
		public String regionFromSpringCloud(RegionProvider regionProvider) {

			// Try to use SpringCloudAWS region
			return regionProvider.getRegion().getName();
		}
	}
}
