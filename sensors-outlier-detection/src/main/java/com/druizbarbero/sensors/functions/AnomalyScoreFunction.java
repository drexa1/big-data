package com.druizbarbero.sensors.functions;

import com.druizbarbero.sensors.data.SingleSensorDataPoint;
import com.druizbarbero.sensors.model.IQRModel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RequiredArgsConstructor
public class AnomalyScoreFunction extends ProcessAllWindowFunction<SingleSensorDataPoint, SingleSensorDataPoint, Window> {
    private final Logger log = LoggerFactory.getLogger(AnomalyScoreFunction.class);

    @NonNull private String sensorKey;

    /**
     * We'll keep this updated with the incoming info
     */
    private transient IQRModel model;

    private SimpleAccumulator proccessedElements = new IntCounter(0);


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        model = new IQRModel(100);
        // register job metrics for this sensor
        getRuntimeContext().addAccumulator("sensor-"+this.sensorKey+"-proccessedElements", this.proccessedElements);
        /* Histogram accumulator only seem to support integers for now -although it should be difficult to handcraft one...
            getRuntimeContext().addAccumulator("sensor-"+this.sensorKey+"-histIQR", new Histogram());
         */
    }

    /**
     * Low-level function, provides access to all the elements of the window being processed.
     * This allows incremental computation of the window while having access to the additional meta information
     * @param context
     * @param elements
     * @param out
     * @throws Exception
     */
    @Override
    public void process(Context context, Iterable<SingleSensorDataPoint> elements, Collector<SingleSensorDataPoint> out) throws Exception {

        Double updatedIQR;
        Float score;

        // for every incoming datapoint
        for (SingleSensorDataPoint dataPoint : elements) {
            // we can't help NULL datapoints
            if(isValidDatapoint(dataPoint)) {
                // push the datapoint to fifo buffer of "windowSize" elements
                updatedIQR = model.push((Float) dataPoint.getValue());
                // we have to wait for the first N elements are in the buffer to get an IQR
                if(!updatedIQR.isNaN()) {
                    score = model.assignScore((Float) dataPoint.getValue(), updatedIQR);
                    dataPoint.setScore(score);
                } else {
                    // what do we do with the first N datapoints??? -until disambiguation, skip
                    dataPoint.setScore(null);
                }
            }

            // valid or not, the datapoint counts as processed and is returned for the next step in the job
            proccessedElements.add(1);
            out.collect(dataPoint);
        }
    }

    /**
     * Validates that the datapoints read from the source contain valid information.
     * Apply here as much validations as are desired...
     * @param dataPoint
     * @return
     */
    private boolean isValidDatapoint(SingleSensorDataPoint dataPoint) {
        return dataPoint.getValue() != null;
    }

    @Override
    public void close() {
        log.debug(" " + proccessedElements.getLocalValue());
    }

}
