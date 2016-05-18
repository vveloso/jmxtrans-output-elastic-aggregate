package com.googlecode.jmxtrans.model.output.elastic;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.googlecode.jmxtrans.exceptions.LifecycleException;
import com.googlecode.jmxtrans.model.Query;
import com.googlecode.jmxtrans.model.Result;
import com.googlecode.jmxtrans.model.Server;
import com.googlecode.jmxtrans.model.ValidationException;
import com.googlecode.jmxtrans.model.output.BaseOutputWriter;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.google.common.base.Optional.fromNullable;

/**
JMXTrans output writer for Elasticsearch which outputs one structured document for each query.
Documents pushed to Elastic have two automatically generated attributes, @metadata and @timestamp. An example of metrics obtained for a JMS queue would be:
<pre>
 {
 	"@timestamp": 1463071268789,
 	"@metadata": {
 		"typeName": "module=Core,type=Queue,address=\"jms.queue.nb8Queue\",name=\"jms.queue.nb8Queue\"",
 		"server": "server.example.com",
 		"typeNameValues": "Queue_\"jms.queue.nb8Queue\"_\"jms.queue.nb8Queue\"",
 		"port": "9990"
 	},
 	"MessagesAdded": 0,
 	"Temporary": false,
 	"ConsumerCount": 20,
 	"DeliveringCount": 0,
 	"Durable": false,
 	"MessageCount": 0,
 	"ScheduledCount": 0
 }
</pre>
 */
public class ElasticAggregateWriter extends BaseOutputWriter {

	private static final Logger LOGGER = LoggerFactory.getLogger(ElasticAggregateWriter.class);

	private static final String DEFAULT_TYPE_NAME = "jmx-entry";
	private static final String DEFAULT_INDEX_NAME = "jmxtrans";
	private static final String DEFAULT_CLUSTER_NAME = "";

	private final String elasticTypeName;
	private final String elasticIndexName;
	private final String url;
	private final String clusterName;
	private final Map<String, Object> settingsMap;
	private ElasticClientConnection client;

	private static final Map<String, ElasticClientConnection> CONNECTIONS = new ConcurrentHashMap<>();

	@JsonCreator
	public ElasticAggregateWriter(@JsonProperty("typeNames") ImmutableList<String> typeNames,
								  @JsonProperty("booleanAsNumber") boolean booleanAsNumber,
								  @JsonProperty("debug") Boolean debugEnabled,
								  @JsonProperty("elasticHostName") String elasticHostName,
								  @JsonProperty("elasticClusterName") String elasticClusterName,
								  @JsonProperty("elasticIndexName") String elasticIndexName,
								  @JsonProperty("elasticTypeName") String elasticTypeName,
								  @JsonProperty("settings") Map<String, Object> settings) {
		super(typeNames, booleanAsNumber, debugEnabled, settings);

		this.settingsMap = ImmutableMap.copyOf(MoreObjects.firstNonNull(settings, Collections.emptyMap()));

		this.elasticIndexName = firstNonNull(elasticIndexName, (String) settingsMap.get("elasticIndexName"), DEFAULT_INDEX_NAME);
		this.elasticTypeName = firstNonNull(elasticTypeName, (String) settingsMap.get("elasticTypeName"), DEFAULT_TYPE_NAME);

		this.url = MoreObjects.firstNonNull(elasticHostName, (String) settingsMap.get("elasticHostName"));
		this.clusterName = firstNonNull(elasticClusterName, (String) settingsMap.get("elasticClusterName"), DEFAULT_CLUSTER_NAME);
	}

	@Override
	public void start() throws LifecycleException {
		super.start();
		LOGGER.info("Starting Elasticsearch writer against {}/{}.", url, clusterName);
		client = CONNECTIONS.computeIfAbsent(url, u -> ElasticClientConnection.build(u, clusterName, settingsMap));
		if (null == client) {
			throw new LifecycleException("Can't start Elasticsearch writer: could not construct a client.");
		} else {
			client.reference();
		}
	}

	@Override
	public void stop() throws LifecycleException {
		super.stop();
		LOGGER.info("Stopping Elasticsearch client against {}/{}.", url, clusterName);
		if (null != client) {
			if (client.release() == 0) {
				LOGGER.info("Discarding Elasticsearch client.");
				CONNECTIONS.remove(client.getHost());
			}
		}
	}

	public void validateSetup(Server server, Query query) throws ValidationException {
		// nothing to validate
	}

	@Override
	protected void internalWrite(Server server, Query query, ImmutableList<Result> results) throws Exception {
		if (client == null) {
			return;
		}

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

		client.addRequest(new IndexRequest(indexName, elasticTypeName).source(document));
	}

}
