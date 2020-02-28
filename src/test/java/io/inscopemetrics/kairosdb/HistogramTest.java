package io.inscopemetrics.kairosdb;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.TreeMap;

public class HistogramTest {
    @Test
    public void testDecodeLargeHistogram() throws JSONException  {
        final JSONObject serialized = new JSONObject();
        serialized.put("min", 1337d);
        serialized.put("max", 1337d);
        serialized.put("sum", 11484742549504d);
        final JSONObject bins = new JSONObject();
        bins.put("1337", 8589934592d);
        serialized.put("bins", bins);
        serialized.put("precision", 7);


        final Histogram h = new Histogram(serialized);

        Assert.assertEquals(1337, (long) h.getMin());
        Assert.assertEquals(1337, (long) h.getMax());
        Assert.assertEquals(11484742549504L, (long) h.getSum());
        Assert.assertEquals(8589934592L, h.getCount());
        final TreeMap<Double, Long> actualBins = h.getBins();
        Assert.assertEquals(8589934592L, actualBins.get(1337d).longValue());
    }
}
