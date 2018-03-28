package com.druizbarbero.sensors.filter;

import com.druizbarbero.sensors.data.SingleSensorDataPoint;
import com.druizbarbero.sensors.jobs.filter.FilterByScore;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.time.Instant;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class FilterByScoreTest {

    FilterByScore filter;

    SingleSensorDataPoint dataPoint;

    @Before
    public void setUp() throws Exception {
        dataPoint = new SingleSensorDataPoint();
        dataPoint.setTimeStampMs(Instant.now().toEpochMilli());
        dataPoint.setValue(100f);
        dataPoint.setSensor("test-sensor");
    }

    @Test
    public void filterByScoreShouldSucceed() throws Exception {
        // given
        dataPoint.setScore(0.5f);
        filter = new FilterByScore(0.5f);
        // when
        boolean result = filter.filter(dataPoint);
        // then
        assertTrue(result);
    }

    @Test
    public void filterByScoreShouldFail() throws Exception {
        // given
        dataPoint.setScore(0.5f);
        filter = new FilterByScore(0.0f);
        // when
        boolean result = filter.filter(dataPoint);
        // then
        assertFalse(result);
    }

}
