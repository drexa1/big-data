package com.druizbarbero.sensors.sinks;

import com.druizbarbero.sensors.data.AbstractDataPoint;
import com.druizbarbero.sensors.data.SingleSensorDataPoint;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class SensorInfluxDBSink<T extends AbstractDataPoint<? extends Number>> extends RichSinkFunction<T> {

    private final Logger log = LoggerFactory.getLogger(SensorInfluxDBSink.class);

    private transient InfluxDB influxDB;
    private String url, username, password;
    private String dbname;

    private final String RETENTION = "autogen";

    private String measurement;
    private final String KEY_TAG = "key";
    private final String VALUE_FIELD = "value";
    private final String SCORE_FIELD = "score";

    public SensorInfluxDBSink(String measurement){
        this.measurement = measurement;
    }

    @Override
    @SuppressWarnings({"deprecation"})
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        url = parameters.getString("influxdb.url", "http://localhost:8086");
        username = parameters.getString("influxdb.username", "admin");
        password = parameters.getString("influxdb.password", "admin");
        dbname = parameters.getString("influxdb.dbname", "intellisense");

        influxDB = InfluxDBFactory.connect(url, username, password);

        // @deprecated (since 2.9, removed in 3.0)
        // Use org.influxdb.InfluxDB.query(Query) to execute a parameterized CREATE DATABASE query.
        if(!influxDB.databaseExists(dbname))
            influxDB.createDatabase(dbname);

        influxDB.setDatabase(dbname);
        influxDB.enableBatch(BatchOptions.DEFAULTS);
    }

    @Override
    public void invoke(T dataPoint, Context context) {
        if(dataPoint instanceof SingleSensorDataPoint && isValidDatapoint((SingleSensorDataPoint) dataPoint)) {
            Point.Builder pBuilder = Point.measurement(measurement)
                    .time(dataPoint.getTimeStampMs(), TimeUnit.MILLISECONDS)
                    .tag(KEY_TAG, ((SingleSensorDataPoint)dataPoint).getSensor())
                    .addField(VALUE_FIELD, dataPoint.getValue())
                    .addField(SCORE_FIELD, ((SingleSensorDataPoint)dataPoint).getScore());

            Point point = pBuilder.build();
            write(point);
        }
    }

    private void write(Point point) {
        try {
            influxDB.write(dbname, RETENTION, point);
        } catch(Exception e) {
            // ...no one likes connection timeouts, but sometimes they happen
            log.error("Something bad happened: ", e.getMessage());
        }
    }

    /**
     * Validate before writing, Influx can't write NULL points.
     * @param dataPoint
     */
    private boolean isValidDatapoint(SingleSensorDataPoint dataPoint) {
        return dataPoint.getValue() != null && dataPoint.getScore() != null;
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

}
