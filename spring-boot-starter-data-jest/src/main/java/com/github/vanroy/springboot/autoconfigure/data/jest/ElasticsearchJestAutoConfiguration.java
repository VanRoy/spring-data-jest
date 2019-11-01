package com.github.vanroy.springboot.autoconfigure.data.jest;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpHost;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.Netty4Plugin;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingClass;
import org.springframework.boot.autoconfigure.data.elasticsearch.ElasticsearchProperties;
import org.springframework.boot.autoconfigure.elasticsearch.jest.HttpClientConfigBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.type.filter.AssignableTypeFilter;
import org.springframework.data.elasticsearch.client.NodeClientFactoryBean;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.SocketUtils;
import org.springframework.util.StringUtils;

/**
 * Jest Elasticsearch configuration.
 * @author Julien Roy
 */
@Configuration
@EnableConfigurationProperties({ElasticsearchJestProperties.class, ElasticsearchProperties.class})
public class ElasticsearchJestAutoConfiguration implements DisposableBean {

	private static final Log logger = LogFactory.getLog(ElasticsearchJestAutoConfiguration.class);

	private static final String INTERNAL_TEST_CLUSTER_NAME = "internal-test-cluster-name";

	@Autowired
	private ElasticsearchJestProperties properties;

	@Autowired(required = false)
	private ElasticsearchProperties esNodeproperties;

	@Autowired(required = false)
	private JestClientFactory jestClientFactory;

	@Autowired(required = false)
	private ObjectProvider<List<HttpClientConfigBuilderCustomizer>> builderCustomizers;

	private Closeable node;

	@Bean
	@ConditionalOnMissingBean(JestClient.class)
	@ConditionalOnMissingClass("org.elasticsearch.node.Node")
	public JestClient client() {
		return createJestClient(properties.getUris());
	}

	@Bean
	@ConditionalOnMissingBean(JestClient.class)
	@ConditionalOnClass(Node.class)
	public JestClient testClient() throws NodeValidationException {

		if (CollectionUtils.isEmpty(properties.getUris())) {
			int httpPort = createInternalNode();
			return createJestClient(Lists.newArrayList("http://localhost:" + httpPort));
		} else {
			return createJestClient(properties.getUris());
		}
	}

	@Override
	public void destroy() throws Exception {
		if (this.node != null) {
			try {
				if (logger.isInfoEnabled()) {
					logger.info("Closing Elasticsearch client");
				}
				this.node.close();
			} catch (Exception ex) {
				if (logger.isErrorEnabled()) {
					logger.error("Error closing Elasticsearch client: ", ex);
				}
			}
		}
	}

	/**
	 * Create Jest client with URI
	 * @param uris URI list of Elasticsearch
	 * @return JestClient
	 */
	private JestClient createJestClient(List<String> uris) {

		HttpClientConfig.Builder builder = new HttpClientConfig.Builder(uris)
			.maxTotalConnection(properties.getMaxTotalConnection())
			.defaultMaxTotalConnectionPerRoute(properties.getDefaultMaxTotalConnectionPerRoute())
			.maxConnectionIdleTime(properties.getMaxConnectionIdleTime(), TimeUnit.MILLISECONDS)
			.readTimeout(properties.getReadTimeout())
			.multiThreaded(properties.getMultiThreaded())
			.defaultSchemeForDiscoveredNodes(properties.getDefaultSchemeForDiscoveredNodes())
			.discoveryEnabled(Boolean.parseBoolean(properties.getDiscoveryEnabled()));

		if (StringUtils.hasText(this.properties.getUsername())) {
			builder.defaultCredentials(this.properties.getUsername(), this.properties.getPassword());
		}

		String proxyHost = this.properties.getProxy().getHost();
		if (StringUtils.hasText(proxyHost)) {
			Integer proxyPort = this.properties.getProxy().getPort();
			Assert.notNull(proxyPort, "Proxy port must not be null");
			builder.proxy(new HttpHost(proxyHost, proxyPort));
		}

		List<HttpClientConfigBuilderCustomizer> configBuilderCustomizers = builderCustomizers != null ? builderCustomizers.getIfAvailable() : new ArrayList<>();
		if (!CollectionUtils.isEmpty(configBuilderCustomizers)) {
			logger.info("Custom HttpClientConfigBuilderCustomizers detected. Applying these to the HttpClientConfig builder.");
			configBuilderCustomizers.stream().forEach(customizer -> customizer.customize(builder));
			logger.info("Custom HttpClientConfigBuilderCustomizers applied.");
		}

		JestClientFactory factory = jestClientFactory != null ? jestClientFactory : new JestClientFactory();
		factory.setHttpClientConfig(builder.build());
		return factory.getObject();
	}

	/**
	 * Create internal Elasticsearch node.
	 * @return HTTP port of node
	 */
	private int createInternalNode() throws NodeValidationException {

		if (logger.isInfoEnabled()) {
			logger.info("Create test ES node");
		}

		int port = SocketUtils.findAvailableTcpPort();
		String clusterName = INTERNAL_TEST_CLUSTER_NAME + UUID.randomUUID();

		Settings.Builder settingsBuilder = Settings.builder()
				.put("cluster.name", clusterName)
				.put("http.type", "netty4")
				.put("http.port", String.valueOf(port));

		if (this.esNodeproperties != null) {
			this.esNodeproperties.getProperties().forEach(settingsBuilder::put);
		}

		Collection<Class<? extends Plugin>> plugins = scanPlugins();
		plugins.add(Netty4Plugin.class);

		this.node = new NodeClientFactoryBean.TestNode(settingsBuilder.build(), plugins).start();

		return Integer.parseInt(settingsBuilder.get("http.port"));
	}

	/**
	 * List all official ES plugins available on ClassPath.
	 * @return List of plugins class
	 */
	@SuppressWarnings("unchecked")
	private static Collection<Class<? extends Plugin>> scanPlugins() {
		ClassPathScanningCandidateComponentProvider componentProvider = new ClassPathScanningCandidateComponentProvider(false);
		componentProvider.addIncludeFilter(new AssignableTypeFilter(Plugin.class));

		return componentProvider.findCandidateComponents("org.elasticsearch.plugin").stream()
				.map(BeanDefinition::getBeanClassName)
				.map(name -> {
					try {
						return (Class<? extends Plugin>) Class.forName(name);
					} catch (ClassNotFoundException e) {
						logger.warn("Cannot load class on plugin detection", e);
						return null;
					}
				})
				.collect(Collectors.toSet());
	}
}