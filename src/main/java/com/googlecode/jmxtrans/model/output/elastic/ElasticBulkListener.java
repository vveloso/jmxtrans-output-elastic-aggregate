package com.googlecode.jmxtrans.model.output.elastic;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Listens to Elastic bulk request events and logs relevant information.
 */
final class ElasticBulkListener implements BulkProcessor.Listener {

	private static final Logger LOGGER = LoggerFactory.getLogger(ElasticBulkListener.class);

	@Override
	public void beforeBulk(long executionId, BulkRequest bulkRequest) {
		LOGGER.debug("Sending Elastic request #{} with {} actions and about {} bytes.",
				executionId, bulkRequest.numberOfActions(), bulkRequest.estimatedSizeInBytes());
	}

	@Override
	public void afterBulk(long executionId, BulkRequest bulkRequest, BulkResponse bulkResponse) {
		bulkResponse.forEach(r -> {
			if (r.isFailed()) {
				LOGGER.warn("Failed to insert documents: {} {}", r.getFailure(), r.getFailureMessage());
			}
		});
	}

	@Override
	public void afterBulk(long executionId, BulkRequest bulkRequest, Throwable throwable) {
		LOGGER.warn("Failed to insert documents.", throwable);
	}
}
