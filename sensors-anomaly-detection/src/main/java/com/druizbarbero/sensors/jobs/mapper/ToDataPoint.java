package com.druizbarbero.sensors.jobs.mapper;

import com.druizbarbero.sensors.data.SingleSensorDataPoint;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.format.DateTimeParseException;

/**
 * Transforms each sensor column from the CSV along with the common timestamp to a POJO.
 */
@RequiredArgsConstructor
public class ToDataPoint implements MapFunction<Tuple2<String, String>, SingleSensorDataPoint> {

    private final Logger log = LoggerFactory.getLogger(ToDataPoint.class);

    @NonNull private String sensorKey;

    @Override
    public SingleSensorDataPoint map(Tuple2<String, String> singleSensorTuple) throws Exception {
        SingleSensorDataPoint dataPoint = new SingleSensorDataPoint();
        dataPoint.setSensor(this.sensorKey);
        try {
            dataPoint.setTimeStampMs(makeTimestamp(singleSensorTuple.f0));
            dataPoint.setValue(makeValue(singleSensorTuple.f1));
        // if something happens, log it and emit an NA point
        } catch(NumberFormatException | DateTimeParseException e) {
            log.error("Error mapping dataPoint " + singleSensorTuple.toString(), e.getMessage());
            dataPoint.setTimeStampMs(null);
            dataPoint.setValue(null);
        }
        return dataPoint;
    }


    private long makeTimestamp(Object timestampField) {
        Instant timestamp = Instant.parse((String) timestampField);
        return timestamp.toEpochMilli();
    }

    private Number makeValue(Object valueField) {
        return Float.parseFloat((String) valueField);
    }

}
