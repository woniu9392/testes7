package com;

import java.net.UnknownHostException;

import javax.annotation.PostConstruct;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Data;

@Data
@Configuration
@ConfigurationProperties(prefix = "es.cluster")
public class ElasticSearchConfig {

	private String hosts;

	private int port;
	
	private int shards;
	
	private int replicas;

	private String scheme = "http";

	private HttpHost[] httpHosts;

	private int connectTimeOut = 90000;

	private int socketTimeout = 30000;

	@PostConstruct
	protected void init() {
		String[] temp = hosts.split(",");
		httpHosts = new HttpHost[temp.length];
		for (int i = 0; i < temp.length; i++) {
			httpHosts[i] = new HttpHost(temp[i], port, scheme);
		}
	}

	public RestHighLevelClient getClient() throws UnknownHostException {
		/** 用户认证对象 */
		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		/** 设置账号密码 */
//		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic", "pyzhpt"));
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic", "3#DajkcCJw@qWzg%"));
		RestClientBuilder restClient = RestClient.builder(httpHosts);
		// 定义监听器，节点出现故障会收到通知。
		restClient.setFailureListener(new RestClient.FailureListener() {
			@Override
			public void onFailure(Node node) {
				super.onFailure(node);
			}
		});
		// 定义节点选择器 这个是跳过data=false，ingest为false的节点
		restClient.setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS);
		// 定义默认请求配置回调
		restClient.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
			@Override
			public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
				return requestConfigBuilder.setConnectTimeout(connectTimeOut) // 链接超时（默认为1秒）
						.setSocketTimeout(socketTimeout); // 套接字超时（默认为30秒）
			}
		}).setHttpClientConfigCallback(new HttpClientConfigCallback() {
			@Override
			public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
				return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
			}
		});
		RestHighLevelClient client = new RestHighLevelClient(restClient);
		return client;
	}
	
	public int getShards() {
		return shards;
	}
	
	public int getReplicas() {
		return replicas;
	}
}
