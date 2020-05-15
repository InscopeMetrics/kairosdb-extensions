/*
 * Copyright 2020 Dropbox Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.inscopemetrics.kairosdb;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import java.util.TreeMap;

import static org.junit.Assert.assertEquals;

/**
 * Test class for {@link Histogram}.
 *
 * @author William Ehlardt (whale at dropbox dot com)
 */
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

        final Histogram h = new Histogram(serialized);

        assertEquals(1337, (long) h.getMin());
        assertEquals(1337, (long) h.getMax());
        assertEquals(11484742549504L, (long) h.getSum());
        assertEquals(8589934592L, h.getCount());
        final TreeMap<Double, Long> actualBins = h.getBins();
        assertEquals(8589934592L, actualBins.get(1337d).longValue());
    }
}
