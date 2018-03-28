package com.druizbarbero.sensors.functions;

import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

import java.time.Instant;

/**
 * Ensures that the watermarks are emitted according to the proper field.
 * The timestamps seem to be a simple timeline, so ASCENDING extractor should do in this case
 */
public class TimestampExtractor extends AscendingTimestampExtractor<Tuple11> {
    @Override
    public long extractAscendingTimestamp(Tuple11 tuple) {
        Instant timestamp = Instant.parse((String) tuple.f0);
        return timestamp.toEpochMilli();
    }
}
