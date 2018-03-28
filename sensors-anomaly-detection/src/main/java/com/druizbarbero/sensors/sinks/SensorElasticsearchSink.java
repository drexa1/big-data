package com.druizbarbero.sensors.sinks;

import com.druizbarbero.sensors.data.SingleSensorDataPoint;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

@RequiredArgsConstructor
public class SensorElasticsearchSink implements ElasticsearchSinkFunction<SingleSensorDataPoint> {
    private static final Logger log = LoggerFactory.getLogger(SensorElasticsearchSink.class);

    @NonNull private String indexName;

    private final String indexType = "sensor";

    @Override
    public void process(SingleSensorDataPoint element, RuntimeContext context, RequestIndexer indexer) {
        try {
            indexer.add(createIndexRequest(element));
        } catch(Exception e) {
            log.error("Something bad happened: ", e.getMessage());
        }
    }

    /**
     * Maps a datapoint to JSON and writes it to the corresponding Elastic index.
     * @param dataPoint
     * @return
     */
    private IndexRequest createIndexRequest(SingleSensorDataPoint dataPoint) {

        Map<String, Object> json = new HashMap<>();
        json.put("timestamp", dataPoint.getTimeStampMs());
        json.put("sensor", dataPoint.getSensor());
        json.put("value", dataPoint.getValue());
        json.put("score", dataPoint.getScore());

        IndexRequest indexRequest = Requests.indexRequest()
                .index(indexName)
                .type(indexType)
                //.id() leave it to Elastic
                .source(json);

        return indexRequest;
    }

}
