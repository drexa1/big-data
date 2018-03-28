package com.druizbarbero.sensors.sinks;

import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.util.ExceptionUtils;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Failover handler, a good place to implement recovery policies or circuit breaker stuff...
 */
public class SensorElasticsearchFailureHandler implements ActionRequestFailureHandler {
    private static final Logger log = LoggerFactory.getLogger(SensorElasticsearchFailureHandler.class);

    @Override
    public void onFailure(ActionRequest action, Throwable failure, int restStatusCode, RequestIndexer indexer) throws Throwable {
        if (ExceptionUtils.findThrowable(failure, EsRejectedExecutionException.class).isPresent()) {
            // full queue; re-add document for indexing
            log.error("SensorElasticsearchFailureHandler failure: EsRejectedExecutionException " + failure.getMessage());
            indexer.add(action);
        } else if (ExceptionUtils.findThrowable(failure, ElasticsearchParseException.class).isPresent()) {
            // malformed document; simply drop request without failing sink
            log.error("SensorElasticsearchFailureHandler failure: ElasticsearchParseException " + failure.getMessage());
        } else {
            // for all other failures, fail the sink
            log.error("SensorElasticsearchFailureHandler failure: others " + failure.getMessage());
            throw failure;
        }
    }

}
