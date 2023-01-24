/*
 * Copyright 2019 InscopeMetrics Inc
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

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.json.JSONException;
import org.json.JSONWriter;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link HistogramDataPointV2Impl} class.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class HistogramDataPointV2ImplTest {

    @Test
    public void testLargeSampleCount() {
        final TreeMap<Double, Long> map = new TreeMap<>();
        map.put(1d, 2147483647L);
        map.put(2d, 2147483647L);
        map.put(3d, 2147483647L);
        map.put(4d, 2147483647L);
        final HistogramDataPointV2Impl dp = new HistogramDataPointV2Impl(1, 7, map, -10, 10, 10, 10);
        assertEquals(8589934588L, dp.getSampleCount());
    }

    @Test
    public void testInfiniteValuesSerializeAsNull() throws JSONException, IOException {
        final TreeMap<Double, Long> map = new TreeMap<>();
        map.put(1d, 1L);
        map.put(2d, 1L);
        map.put(3d, 1L);
        map.put(4d, 1L);
        final HistogramDataPointV2Impl dp = new HistogramDataPointV2Impl(
                1,
                7,
                map,
                Double.NEGATIVE_INFINITY,
                Double.POSITIVE_INFINITY,
                Double.NEGATIVE_INFINITY,
                Double.POSITIVE_INFINITY);
        assertEquals(4, dp.getSampleCount());
        final StringWriter writer = new StringWriter();
        final JSONWriter jsonWriter = new JSONWriter(writer);
        dp.writeValueToJson(jsonWriter);
        final Gson gson = new Gson();
        final JsonObject result = gson.fromJson(writer.toString(), JsonObject.class);
        assertTrue(result.get("min").isJsonNull());
        assertTrue(result.get("max").isJsonNull());
        assertTrue(result.get("mean").isJsonNull());
        assertTrue(result.get("sum").isJsonNull());

    }
}
