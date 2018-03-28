package com.druizbarbero.sensors.mapper;

import com.druizbarbero.sensors.data.AbstractDataPoint;
import com.druizbarbero.sensors.jobs.mapper.ToDataPoint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class ToDataPointTest {

    ToDataPoint mapper;

    @Before
    public void setUp() {
        mapper = new ToDataPoint("test-sensor");
    }

    @Test
    public void toDataPointShouldSucceed() throws Exception {
        // given
        Tuple2<String, String> tuple = new Tuple2<>("2017-03-20T07:11:00Z", "201.30312");
        // when
        AbstractDataPoint<Number> datapoint = mapper.map(tuple);
        // then
        assertEquals("201.30312", datapoint.getValue().toString());
    }

    @Test
    public void toDataPointShouldFailAndEmitNAPoint() throws Exception {
        // given
        Tuple2<String, String> tuple = new Tuple2<>("2017-03-20T07:11:00Z", "~~~");
        // when
        AbstractDataPoint<Number> datapoint = mapper.map(tuple);
        // then
        assertNull(datapoint.getValue());
    }

}
