package com.druizbarbero.sensors.jobs.filter;

import com.druizbarbero.sensors.data.SingleSensorDataPoint;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.functions.FilterFunction;

@RequiredArgsConstructor
public class FilterByScore implements FilterFunction<SingleSensorDataPoint> {

    @NonNull private Float score;

    @Override
    public boolean filter(SingleSensorDataPoint singleSensorDataPoint) throws Exception {
        return this.score.equals(singleSensorDataPoint.getScore()); // remember that the first 100 points are intended for the IQR and don't have score yet
    }

}
