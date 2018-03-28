package com.druizbarbero.sensors.model;

import lombok.NonNull;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.util.stream.Stream;

/**
 * Maintains a buffer of the last N elements, and provides a function such as the IQR to facilitate outliers spotting.
 * @param <T extends Number>
 */
public class IQRModel<T extends Number> {

    /**
     * Bounded fifo buffer of N numeric elements.
     */
    @NonNull CircularFifoQueue buffer;

    /**
     * Returns the actual IRQ value for the current buffer contents.
     */
    public Double currentIQR = new Double(0L);

    DescriptiveStatistics stats;

    public IQRModel(int bufferLenght) {
        buffer = new CircularFifoQueue(bufferLenght);
    }

    /**
     * Pushes a new element to the buffer and recomputes the IRQ with its current contents.
     * @param element
     */
    public Double push(Number element) {
        buffer.add(element);
        this.currentIQR = computeIQR();
        return this.currentIQR;
    }

    /**
     * Computes the (Q3-Q1) of a set of elements in order to spot outliers.
     * @return IQR
     */
    public Double computeIQR() {
        // until we have the first N elements, we return a distinguishable out of range value
        if(!buffer.isAtFullCapacity())
            return Double.NaN;

        double[] bufferContent = getBufferContent(buffer.toArray());
        stats = new DescriptiveStatistics(bufferContent);

        Double IQR = stats.getPercentile(75) - stats.getPercentile(25);
        return IQR;
    }

    /**
     * Returns the contents of the buffer in the shape needed by the DescriptiveStatistics class.
     * @return
     */
    public double[] getBufferContent(Object[] array) {
        return Stream.of(array).mapToDouble(n -> ((Number) n).doubleValue()).toArray();
    }

    /**
     * Tags a datapoint with a score based on the recorded IQR of the sensor.
     * @param value
     * @param currentIQR
     * @return
     */
    public Float assignScore(Number value, Double currentIQR) {
        if(value.doubleValue() < 1.5*currentIQR )
            return 0.0f;
        if(value.doubleValue() >= 1.5*currentIQR && value.doubleValue() < 3*currentIQR)
            return 0.5f;
        if(value.doubleValue() >= 3*currentIQR)
            return 1.0f;

        return null;
    }

}
