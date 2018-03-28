package com.druizbarbero.sensors.jobs;

import com.druizbarbero.sensors.data.SingleSensorDataPoint;
import com.druizbarbero.sensors.data.input.SensorsTupleType;
import com.druizbarbero.sensors.functions.AnomalyScoreFunction;
import com.druizbarbero.sensors.jobs.builder.StreamBuilder;
import com.druizbarbero.sensors.jobs.filter.FilterByScore;
import com.druizbarbero.sensors.jobs.mapper.ToDataPoint;
import com.druizbarbero.sensors.sinks.SensorElasticsearchFailureHandler;
import com.druizbarbero.sensors.sinks.SensorElasticsearchSink;
import com.druizbarbero.sensors.sinks.SensorInfluxDBSink;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class MainJob {
    private static final Logger log = LoggerFactory.getLogger(MainJob.class);

    private static String configFilePath = "src/main/resources/application.yml";
    private static String inputFilePath  = "src/main/resources/sensors.csv";

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromPropertiesFile(configFilePath);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // CONFIG PARAMS
        final int windowSize = params.getInt("flink.window.elements", 100);
        final int slideSize = params.getInt("flink.window.slide", 1);

        final boolean withElastic = params.getBoolean("job.enableElastic", false);
        final String esClusterAddress = params.get("elastic.cluster.address", "127.0.0.1");
        final int esClusterPort = params.getInt("elastic.cluster.port", 9300);
        final String esClusterName = params.get("elastic.cluster.name", "elasticsearch");
        final String esDoNotBuffer = params.get("elastic.bulk.flush.max.actions", "1");
        final String retryOnRejection = params.get("elastic.bulk.flush.backoff.enable", "true");
        final String retryAttemps = params.get("elastic.bulk.flush.backoff.retries", "3");

        List<InetSocketAddress> transportAddresses = new ArrayList<>();
        Map<String, String> esConfig = new HashMap<>();
        if(withElastic) {
            transportAddresses.add(new InetSocketAddress(InetAddress.getByName(esClusterAddress), esClusterPort));
            esConfig.put("cluster.name", esClusterName);
            esConfig.put("bulk.flush.max.actions", esDoNotBuffer); // emit after every element
            esConfig.put("bulk.flush.backoff.enable", retryOnRejection);
            esConfig.put("bulk.flush.backoff.retries", retryAttemps);
        }

        final boolean withGrafana = params.getBoolean("job.enableGrafana", false);

        // ENV CONFIG
        env.setParallelism(params.getInt("parallelism", 1));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // make parameters accessible from the cluster web-ui
        env.getConfig().setGlobalJobParameters(params);


        StreamBuilder streamBuilder = new StreamBuilder(env);
        DataStream stream = streamBuilder.build(inputFilePath, true);

        // Process as many sensors as we have
        int nSensors = SensorsTupleType.tupleTypeInfo.getTotalFields();
        DataStream[] allSensors = new DataStream[nSensors];
        IntStream.range(1, nSensors).forEach(sensor -> {

                    allSensors[sensor] = stream
                            // select the timestamp and the data column of each
                            .project(0, sensor).uid("op-select-s" + sensor)
                            .map(new ToDataPoint("sensor-" + sensor)).uid("op-toDataPoint-s" + sensor)
                            .keyBy("sensor")
                            // this window will be triggered every "slideSize"-elements and the computation will take into account the last “windowSize” elements
                            .countWindowAll(windowSize, slideSize)
                            .process(new AnomalyScoreFunction("sensor-" + sensor)).uid("op-scoreAnomaly-s" + sensor);
                    if (withGrafana) {
                        allSensors[sensor]
                                .addSink(new SensorInfluxDBSink("sensors")).uid("op-influxdb-s" + sensor);
                    }
                    if(withElastic) {
                        allSensors[sensor]
                                // here we can apply all the filtering desired to route to different indexes...
                                //.filter(new FilterByScore(0.5f))
                                .addSink(new ElasticsearchSink(esConfig, transportAddresses, new SensorElasticsearchSink("sensor-all"), new SensorElasticsearchFailureHandler()))
                                .uid("op-elastic-s" + sensor);
                    }
                    boolean printSink = false;
                    if(printSink) {
                        allSensors[sensor].print().uid("op-dummyprint-s" + sensor);
                    }
        });

        JobExecutionResult results = env.execute();
        log.info(results.getAllAccumulatorResults().toString());
    }

}
