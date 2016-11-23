package com.github.vanroy.springboot.autoconfigure.data.jest;

import java.util.Collection;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import org.elasticsearch.Version;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.plugins.Plugin;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingClass;
import org.springframework.boot.autoconfigure.data.elasticsearch.ElasticsearchProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.type.filter.AssignableTypeFilter;
import org.springframework.util.ReflectionUtils;
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

	private Releasable releasable;

	@Bean
	@ConditionalOnMissingBean(JestClient.class)
	@ConditionalOnMissingClass("org.elasticsearch.node.Node")
	public JestClient client() {
		return createJestClient(properties.getUri());
	}

	@Bean
	@ConditionalOnMissingBean(JestClient.class)
	@ConditionalOnClass(Node.class)
	public JestClient testClient() {

		if (StringUtils.isEmpty(properties.getUri())) {
			int httpPort = createInternalNode();
			return createJestClient("http://localhost:" + httpPort);
		} else {
			return createJestClient(properties.getUri());
		}
	}

	@Override
	public void destroy() throws Exception {
		if (this.releasable != null) {
			try {
				if (logger.isInfoEnabled()) {
					logger.info("Closing Elasticsearch client");
				}

				try {
					this.releasable.close();
				} catch (NoSuchMethodError var2) {
					ReflectionUtils.invokeMethod(ReflectionUtils.findMethod(Releasable.class, "release"), this.releasable);
				}
			} catch (Exception var3) {
				if (logger.isErrorEnabled()) {
					logger.error("Error closing Elasticsearch client: ", var3);
				}
			}
		}
	}

	/**
	 * Create Jest client with URI
	 * @param uri URI of Elasticsearch
	 * @return JestClient
	 */
	private JestClient createJestClient(String uri) {

		HttpClientConfig.Builder builder = new HttpClientConfig.Builder(uri)
			.maxTotalConnection(properties.getMaxTotalConnection())
			.defaultMaxTotalConnectionPerRoute(properties.getDefaultMaxTotalConnectionPerRoute())
			.readTimeout(properties.getReadTimeout())
			.multiThreaded(properties.getMultiThreaded());

		if (StringUtils.hasText(this.properties.getUsername())) {
			builder.defaultCredentials(this.properties.getUsername(), this.properties.getPassword());
		}

		JestClientFactory factory = jestClientFactory != null ? jestClientFactory : new JestClientFactory();
		factory.setHttpClientConfig(builder.build());
		return factory.getObject();
	}

	/**
	 * Create internal Elasticsearch node.
	 * @return HTTP port of node
	 */
	private int createInternalNode() {

		if (logger.isInfoEnabled()) {
			logger.info("Create test ES node");
		}

		int port = SocketUtils.findAvailableTcpPort();

		Settings.Builder settingsBuilder = Settings.settingsBuilder().
				put("http.enabled", String.valueOf(true)).
				put("http.port", String.valueOf(port));

		if (this.esNodeproperties != null) {
			settingsBuilder.put(this.esNodeproperties.getProperties());
		}

		String clusterName = INTERNAL_TEST_CLUSTER_NAME + UUID.randomUUID();

		Settings settings = new NodeBuilder().settings(settingsBuilder).clusterName(clusterName).local(true).getSettings().build();

        Version version = properties.getInternalNodeVersion() == null ? Version.CURRENT : Version.fromString(properties.getInternalNodeVersion());

		this.releasable = new InternalNode(settings, version, scanPlugins()).start();

		return port;
	}

	/**
	 * List all official ES plugins availble on ClassPath.
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

	/**
	 * Specific InternalNode class used to specify plugins and version.
	 */
	private static class InternalNode extends Node {

		private final Version version;

		InternalNode(Settings settings, Version version, Collection<Class<? extends Plugin>> classpathPlugins) {
			super(InternalSettingsPreparer.prepareEnvironment(settings, null), version, classpathPlugins);
			this.version = version;
		}

		public Version getVersion() {
			return this.version;
		}
	}

}