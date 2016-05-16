package com.googlecode.jmxtrans.model.output.elastic;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.googlecode.jmxtrans.exceptions.LifecycleException;
import com.googlecode.jmxtrans.model.Query;
import com.googlecode.jmxtrans.model.Result;
import com.googlecode.jmxtrans.model.Server;
import com.googlecode.jmxtrans.model.ValidationException;
import com.googlecode.jmxtrans.model.output.BaseOutputWriter;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Index;
import org.apache.http.annotation.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * Created by vveloso on 12-05-2016.
 */
@NotThreadSafe
public class ElasticAggregateWriter extends BaseOutputWriter {

	private static final Logger LOGGER = LoggerFactory.getLogger(ElasticAggregateWriter.class);

	private static final String DEFAULT_TYPE_NAME = "jmx-entry";
	private static final String DEFAULT_INDEX_NAME = "jmxtrans";
	private static final Integer DEFAULT_READ_TIMEOUT = 10000;

	private final String elasticTypeName;
	private final String elasticIndexName;
	private final JestClient jestClient;

	@JsonCreator
	public ElasticAggregateWriter(@JsonProperty("typeNames") ImmutableList<String> typeNames,
								  @JsonProperty("booleanAsNumber") boolean booleanAsNumber,
								  @JsonProperty("debug") Boolean debugEnabled,
								  @JsonProperty("connectionUrl") String connectionUrl,
								  @JsonProperty("connectionReadTimeout") Integer connectionReadTimeout,
								  @JsonProperty("elasticTypeName") String elasticTypeName,
								  @JsonProperty("elasticIndexName") String elasticIndexName,
								  @JsonProperty("settings") Map<String, Object> settings) {
		super(typeNames, booleanAsNumber, debugEnabled, settings);
		final Map<String, Object> settingsMap = MoreObjects.firstNonNull(settings, Collections.emptyMap());

		this.elasticIndexName = firstNonNull(elasticIndexName, (String) settingsMap.get("elasticIndexName"), DEFAULT_INDEX_NAME);
		this.elasticTypeName = firstNonNull(elasticTypeName, (String) settingsMap.get("elasticTypeName"), DEFAULT_TYPE_NAME);

		int readTimeout = MoreObjects.firstNonNull(connectionReadTimeout, DEFAULT_READ_TIMEOUT);

		this.jestClient = createJestClient(connectionUrl, readTimeout);
	}

	private JestClient createJestClient(String connectionUrl, int readTimeout) {
		LOGGER.info("Creating a jest elastic search client for connection URL: {} with timeout: {}",
				connectionUrl, readTimeout);
		JestClientFactory factory = new JestClientFactory();
		factory.setHttpClientConfig(
				new HttpClientConfig.Builder(connectionUrl)
						.readTimeout(readTimeout)
						.multiThreaded(true)
						.build());
		return factory.getObject();
	}

	@Override
	public void start() throws LifecycleException {
		super.start();

	}

	@Override
	public void stop() throws LifecycleException {
		super.stop();

		jestClient.shutdownClient();
	}

	public void validateSetup(Server server, Query query) throws ValidationException {
		// nothing to validate
	}

	@Override
	protected void internalWrite(Server server, Query query, ImmutableList<Result> results) throws Exception {
		if (results.isEmpty()) {
			LOGGER.debug("Not processing empty query result.");
			return;
		}

		final Map<String, List<Result>> resultsMap = results.stream()
				.collect(Collectors.groupingBy(Result::getTypeName));

		LOGGER.debug("Query results: {}", resultsMap);

		resultsMap.forEach((t, r) -> write(server, t, r));
	}

	private void write(Server server, String typeName, List<Result> results) {
		final Map<String, Object> document = new HashMap<>();
		final Map<String, Object> metadata = new HashMap<>(6);
		final String typeNameValues = getConcatedTypeNameValues(typeName);
		final String indexName = String.format(elasticIndexName, Calendar.getInstance());

		metadata.put("serverAlias", server.getAlias());
		metadata.put("server", server.getHost());
		metadata.put("port", Integer.valueOf(server.getPort()));
		metadata.put("typeName", typeName);
		metadata.put("typeNameValues", typeNameValues);

		for (final Result result : results) {
			metadata.putIfAbsent("className", result.getClassName());
			document.putIfAbsent("@timestamp", result.getEpoch());

			final ImmutableMap<String, Object> values = result.getValues();
			document.put(
					fromNullable(result.getKeyAlias()).or(result.getAttributeName()),
					values.size() == 1 ? values.get(result.getAttributeName()) : values);
		}

		document.put("@metadata", metadata);

		LOGGER.debug("Insert into Elastic index [{}] with type [{}]: {}", indexName, elasticTypeName, document);

		final Index idx = new Index.Builder(document).index(indexName).type(elasticTypeName).build();
		try {
			final DocumentResult result = jestClient.execute(idx);
			if (!result.isSucceeded()) {
				LOGGER.error("Failed to write entry to Elastic: {}", result.getErrorMessage());
			}
		} catch (IOException e) {
			LOGGER.error("Failed to write entry to Elastic due to an exception: {}", e.getMessage());
		}
	}

}
