/*
 * Copyright 2017 SmartSheet.com
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
package io.inscopemetrics.kairosdb.integration;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import io.inscopemetrics.kairosdb.Histogram;
import io.inscopemetrics.kairosdb.KairosHelper;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * Tests for storing histogram datapoints.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
public final class StorageIT {
    private final CloseableHttpClient client = HttpClients.createDefault();

    @Test
    public void testStoreDataPointNoPrecision() throws JSONException, IOException {
        final Iterable<Double> dataPoints = Arrays.asList(1d, 3d, 5d, 7d, 9d, 1d, 9d);
        final Histogram histogramWithoutPrecision = new Histogram(dataPoints);
        final Histogram histogramWithDefaultPrecision = new Histogram(dataPoints, (byte) 7);
        final int timestamp = 10;
        final String metricName = "testStoreDataPointNoPrecision_histogram";

        final HttpPost queryRequest = KairosHelper.queryFor(timestamp, timestamp, metricName);
        KairosHelper.postAndVerifyHistogram(
                client,
                queryRequest,
                timestamp,
                histogramWithoutPrecision,
                metricName,
                response -> {
                    try {
                        assertEquals(200, response.getStatusLine().getStatusCode());
                        final String body = CharStreams.toString(new InputStreamReader(response.getEntity().getContent(), Charsets.UTF_8));
                        final JSONObject responseJson = new JSONObject(body);
                        final JSONObject queryObject = responseJson.getJSONArray("queries").getJSONObject(0);
                        assertEquals(1, queryObject.getInt("sample_size"));
                        final JSONArray result = queryObject.getJSONArray("results")
                                .getJSONObject(0)
                                .getJSONArray("values")
                                .getJSONArray(0);
                        assertEquals(timestamp, result.getInt(0));
                        final JSONObject histogramJson = result.getJSONObject(1);

                        // The returned histogram should have defaulted the precision
                        final Histogram returnHistogram = new Histogram(histogramJson);
                        assertEquals(histogramWithDefaultPrecision, returnHistogram);
                    } catch (final IOException | JSONException e) {
                        throw new RuntimeException(e);
                    }
            });
    }

    @Test
    public void testStoreDataPoint() throws IOException, JSONException {
        final Histogram histogram = new Histogram(Arrays.asList(1d, 3d, 5d, 7d, 9d, 1d, 9d), (byte) 6);
        final int timestamp = 10;
        final String metricName = "testStoreDataPoint_histogram";

        final HttpPost queryRequest = KairosHelper.queryFor(timestamp, timestamp, metricName);
        KairosHelper.postAndVerifyHistogram(
                client,
                queryRequest,
                timestamp,
                histogram,
                metricName,
                response -> {
                    try {
                        assertEquals(200, response.getStatusLine().getStatusCode());
                        final String body = CharStreams.toString(new InputStreamReader(response.getEntity().getContent(), Charsets.UTF_8));
                        final JSONObject responseJson = new JSONObject(body);
                        final JSONObject queryObject = responseJson.getJSONArray("queries").getJSONObject(0);
                        assertEquals(1, queryObject.getInt("sample_size"));
                        final JSONArray result = queryObject.getJSONArray("results")
                                .getJSONObject(0)
                                .getJSONArray("values")
                                .getJSONArray(0);
                        assertEquals(timestamp, result.getInt(0));
                        final JSONObject histogramJson = result.getJSONObject(1);
                        final Histogram returnHistogram = new Histogram(histogramJson);
                        assertEquals(histogram, returnHistogram);
                    } catch (final IOException | JSONException e) {
                        throw new RuntimeException(e);
                    }
                });
    }
}
