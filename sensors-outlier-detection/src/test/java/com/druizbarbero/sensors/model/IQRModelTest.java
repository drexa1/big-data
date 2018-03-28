package com.druizbarbero.sensors.model;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.stream.Stream;

import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class IQRModelTest {

    IQRModel model;

    Object[] testContents = {7.0f, 7.0f, 31.0f, 31.0f, 47.0f, 75.0f, 87.0f, 115.0f, 116.0f, 119.0f, 119.0f, 155.0f, 177.0f};

    @Test
    public void getBufferContentShouldSucceedWhileInputIsNumbers() throws Exception {
        // given
        Object[] handleIt = {7.0f, 7.0};
        model = new IQRModel(handleIt.length);
        // when
        double[] result = model.getBufferContent(handleIt);
        // then
        assertSame(handleIt.length, result.length);
    }

    @Test(expected = ClassCastException.class)
    public void getBufferContentShouldFail() throws Exception {
        // given
        Object[] breakIt = {7.0f, "7.0"};
        model = new IQRModel(breakIt.length);
        // when
        double[] result = model.getBufferContent(breakIt);
    }

    @Test
    public void fifoBufferShouldSucceed() throws Exception {
        // given
        model = new IQRModel(3);
        // when
        model.push(1.0f);
        model.push(2.0f);
        model.push(3.0f);
        model.push(4.0f);
        // then
        Object[] expected = {2.0f, 3.0f, 4.0f};
        assertArrayEquals(model.buffer.toArray(), expected);
    }

    @Test
    public void whileBufferIsNotFullIQRshouldBeNaN() throws Exception{
        // given
        model = new IQRModel(3);
        // when
        model.push(1.0f);
        model.push(2.0f);
        Double currentIQR = model.currentIQR;
        // then
        assertTrue(currentIQR.isNaN());
    }

    @Test
    public void computeIQRshouldSucceed() throws Exception {
        // given
        model = new IQRModel(testContents.length);
        // when
        Stream.of(testContents).forEach(e -> model.push((Number) e));
        Double currentIQR = model.currentIQR;
        // then
        assertEquals(currentIQR.longValue(), 88);
    }

}
