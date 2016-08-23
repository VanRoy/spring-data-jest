package com.github.vanroy.springboot.autoconfigure.data.jest;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Jest Elasticsearch properties.
 * @author Julien Roy
 */
@ConfigurationProperties(prefix = "spring.data.jest")
public class ElasticsearchJestProperties {

	private String uri;
	private String username;
	private String password;

	private String awsRegion;

	private int maxTotalConnection = 50;
	private int defaultMaxTotalConnectionPerRoute = 50;
	private int readTimeout = 5000;
	private Boolean multiThreaded = true;

	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
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
}
