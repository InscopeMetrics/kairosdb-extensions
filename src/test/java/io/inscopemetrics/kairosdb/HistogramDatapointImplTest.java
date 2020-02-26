package io.inscopemetrics.kairosdb;

import org.junit.Assert;
import org.junit.Test;

import java.util.TreeMap;

public class HistogramDatapointImplTest {
    @Test
    public void testLargeSampleCount() {
        final TreeMap<Double, Integer> map = new TreeMap<>();
        map.put(1d, 2147483647);
        map.put(2d, 2147483647);
        map.put(3d, 2147483647);
        map.put(4d, 2147483647);
        final HistogramDataPointImpl dp = new HistogramDataPointImpl(1, map, -10, 10, 10, 10);
        Assert.assertEquals(8589934588L, dp.getSampleCount());
    }
}
