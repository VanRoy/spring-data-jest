package com.github.vanroy.springboot.autoconfigure.data.jest;

import java.util.UUID;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingClass;
import org.springframework.boot.autoconfigure.data.elasticsearch.ElasticsearchDataAutoConfiguration;
import org.springframework.boot.autoconfigure.data.elasticsearch.ElasticsearchProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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

	private JestClient createJestClient(String uri) {

		HttpClientConfig.Builder builder = new HttpClientConfig.Builder(uri)
			.maxTotalConnection(properties.getMaxTotalConnection())
			.defaultMaxTotalConnectionPerRoute(properties.getDefaultMaxTotalConnectionPerRoute())
			.readTimeout(properties.getReadTimeout())
			.multiThreaded(properties.getMultiThreaded());

		if (StringUtils.hasText(this.properties.getUsername())) {
			builder.defaultCredentials(this.properties.getUsername(), this.properties.getPassword());
		}

		JestClientFactory factory = new JestClientFactory();
		factory.setHttpClientConfig(builder.build());
		return factory.getObject();
	}

	private int createInternalNode() {

		if (logger.isInfoEnabled()) {
			logger.info("Create test ES node");
		}

		int port = SocketUtils.findAvailableTcpPort();

		Settings.Builder settings = Settings.settingsBuilder().
				put("http.enabled", String.valueOf(true)).
				put("http.port", String.valueOf(port));

		if (this.esNodeproperties != null) {
			settings.put(this.esNodeproperties.getProperties());
		}

		String clusterName = INTERNAL_TEST_CLUSTER_NAME + UUID.randomUUID();

		this.releasable = new NodeBuilder().settings(settings).clusterName(clusterName).local(true).node();

		return port;
	}

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

}