package com.googlecode.jmxtrans.model.output.elastic;

import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Holds together an Elastic transport client and a request processor, with reference counting.
 */
final class ElasticClientConnection {
	private static final Logger LOGGER = LoggerFactory.getLogger(ElasticClientConnection.class);

	private static final int ELASTIC_PORT_1 = 9200;
	private static final int ELASTIC_PORT_2 = 9201;

	private final AtomicInteger refCount = new AtomicInteger(0);
	private final String host;
	private final RestHighLevelClient client;
	private final BulkProcessor processor;

	private ElasticClientConnection(String host, RestHighLevelClient client, BulkProcessor bulkProcessor) {
		this.host = host;
		this.client = client;
		this.processor = bulkProcessor;
	}

	void addRequest(IndexRequest indexRequest) {
		processor.add(indexRequest);
	}

	String getHost() {
		return host;
	}

	int reference() {
		return refCount.incrementAndGet();
	}

	int release() {
		final int value = refCount.decrementAndGet();
		if (0 == value) {
			try {
				LOGGER.info("Flushing Elastic requests for {}.", host);
				if (!processor.awaitClose(5, TimeUnit.MINUTES)) {
					LOGGER.warn("Some Elastic requests were still pending.");
				}
			} catch (InterruptedException e) {
				LOGGER.error("An error occurred while flushing requests.", e);
			}
			LOGGER.info("Closing Elastic client for {}.", host);
			try {
				client.close();
			} catch (IOException e) {
				LOGGER.error("Failed to close Elastic client for {}", host);
			}
		}
		return value;
	}

	static ElasticClientConnection build(String elasticHostName, String clusterName, Map<String, Object> settings) {
		final RestHighLevelClient elasticClient = createElasticClient(elasticHostName, clusterName);
		final BulkProcessor elasticProcessor = createElasticProcessor(elasticClient, settings);
		return new ElasticClientConnection(elasticHostName, elasticClient, elasticProcessor);
	}

	private static RestHighLevelClient createElasticClient(String elasticHostName, String clusterName) {
		LOGGER.info("Creating Elasticsearch client against {}:{} on cluster '{}'", elasticHostName, ELASTIC_PORT_1, clusterName);
		try {
			final InetAddress address = InetAddress.getByName(elasticHostName);

			return new RestHighLevelClient(
					RestClient.builder(
							new HttpHost(address, ELASTIC_PORT_1, "http"),
							new HttpHost(address, ELASTIC_PORT_2, "http")
					)
			);

		} catch (UnknownHostException e) {
			LOGGER.error("Unknown host: {}", elasticHostName);
			return null;
		}
	}

	private static BulkProcessor createElasticProcessor(RestHighLevelClient client, Map<String, Object> settings) {

		final Integer maxBulkRequests = (Integer) settings.getOrDefault("maxBulkRequests", 5000);
		final Integer maxBulkSize = (Integer) settings.getOrDefault("maxBulkSizeMB", 100);
		final Integer maxBulkHoldSeconds = (Integer) settings.getOrDefault("maxBulkHoldSeconds", 15);
		final Integer bulkBackoffWaitMillis = (Integer) settings.getOrDefault("bulkBackoffWaitMillis", 100);
		final Integer maxBulkBackoffRetries = (Integer) settings.getOrDefault("maxBulkBackoffRetries", 3);
		final Integer bulkConcurrency = (Integer) settings.getOrDefault("bulkConcurrency", 1);

		LOGGER.info("Creating processor: {} actions, {} concurrent, {}MB size, flush every {}s, backoff @ {}ms w/ {} retries.",
				maxBulkRequests, bulkConcurrency, maxBulkSize, maxBulkHoldSeconds, bulkBackoffWaitMillis, maxBulkBackoffRetries);

		// see https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/java-docs-bulk-processor.html
		return BulkProcessor.builder(client::bulkAsync, new ElasticBulkListener())
				// We want to execute the bulk every 5000 requests.
				.setBulkActions(maxBulkRequests)
				// We want to flush the bulk every 100MB.
				.setBulkSize(new ByteSizeValue(maxBulkSize, ByteSizeUnit.MB))
				// We want to flush the bulk every 15 seconds whatever the number of requests.
				.setFlushInterval(TimeValue.timeValueSeconds(maxBulkHoldSeconds))
				// A value of 1 means 1 concurrent request is allowed to be executed while accumulating new bulk requests.
				.setConcurrentRequests(bulkConcurrency)
				// Set a custom backoff policy which will initially wait for 100ms, increase exponentially and retries up to three times.
				.setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(bulkBackoffWaitMillis), maxBulkBackoffRetries))
				.build();
	}

}
