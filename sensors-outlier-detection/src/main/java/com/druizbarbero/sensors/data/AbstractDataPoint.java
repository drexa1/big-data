package com.druizbarbero.sensors.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public abstract class AbstractDataPoint<T> {

    /**
    * Timestamp of the measurement in milliseconds.
    */
    private Long timeStampMs;

    /**
    * Actual value of the measurement.
    */
    private T value;

}
