package com.github.vanroy.springboot.autoconfigure.data.jest;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Collections;
import java.util.List;

/**
 * Jest Elasticsearch properties.
 * @author Julien Roy
 */
@ConfigurationProperties(prefix = "spring.data.jest")
public class ElasticsearchJestProperties {

	private List<String> uris;
	private String username;
	private String password;

	private String awsRegion;

	private int maxTotalConnection = 50;
	private int defaultMaxTotalConnectionPerRoute = 50;
	private int readTimeout = 5000;
	private long maxConnectionIdleTime = 0L; // Idle connection reaping disabled by default
	private Boolean multiThreaded = true;
	
	/**
	 * Proxy settings.
	 */
	private final Proxy proxy = new Proxy();
	
	public Proxy getProxy() {
		return this.proxy;
	}

	public List<String> getUris() {
		return uris;
	}

	public void setUris(List<String> uris) {
		this.uris = uris;
	}

	public void setUri(String uri) {
		this.uris = Collections.singletonList(uri);
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public int getMaxTotalConnection() {
		return maxTotalConnection;
	}

	public void setMaxTotalConnection(int maxTotalConnection) {
		this.maxTotalConnection = maxTotalConnection;
	}

	public int getDefaultMaxTotalConnectionPerRoute() {
		return defaultMaxTotalConnectionPerRoute;
	}

	public void setDefaultMaxTotalConnectionPerRoute(int defaultMaxTotalConnectionPerRoute) {
		this.defaultMaxTotalConnectionPerRoute = defaultMaxTotalConnectionPerRoute;
	}

	public int getReadTimeout() {
		return readTimeout;
	}

	public void setReadTimeout(int readTimeout) {
		this.readTimeout = readTimeout;
	}

	public long getMaxConnectionIdleTime() {
		return maxConnectionIdleTime;
	}

	public void setMaxConnectionIdleTime(long maxConnectionIdleTime) {
		this.maxConnectionIdleTime = maxConnectionIdleTime;
	}

	public Boolean getMultiThreaded() {
		return multiThreaded;
	}

	public void setMultiThreaded(Boolean multiThreaded) {
		this.multiThreaded = multiThreaded;
	}

	public String getAwsRegion() {
		return awsRegion;
	}

	public void setAwsRegion(String awsRegion) {
		this.awsRegion = awsRegion;
	}
	
	public static class Proxy {

		/**
		 * Proxy host the HTTP client should use.
		 */
		private String host;

		/**
		 * Proxy port the HTTP client should use.
		 */
		private Integer port;

		public String getHost() {
			return this.host;
		}

		public void setHost(String host) {
			this.host = host;
		}

		public Integer getPort() {
			return this.port;
		}

		public void setPort(Integer port) {
			this.port = port;
		}

	}
}
