/*
 * Copyright 2018 Inscope Metrics
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
package io.inscopemetrics.kairosdb.performance;

import com.arpnetworking.metrics.generator.metric.GaussianCountMetricGenerator;
import com.arpnetworking.metrics.generator.metric.GaussianMetricGenerator;
import com.arpnetworking.metrics.generator.name.SingleNameGenerator;
import com.google.gson.JsonObject;
import io.inscopemetrics.kairosdb.HistogramFormingMetrics;
import org.apache.commons.math3.random.MersenneTwister;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datapoints.DataPointFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Performance test for data point factories.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
public abstract class BaseFactoryTestPerf {

    private static final MersenneTwister RANDOM = new MersenneTwister(0);
    private static final GaussianMetricGenerator GAUSSIAN_METRIC_GENERATOR =
            new GaussianMetricGenerator(300, 90, new SingleNameGenerator(RANDOM), RANDOM);
    private static final GaussianCountMetricGenerator GENERATOR =
            new GaussianCountMetricGenerator(20000, 12000, GAUSSIAN_METRIC_GENERATOR, RANDOM);

    private static final Collection<JsonObject> DATA_POINTS_JSON;

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    static {
        final List<JsonObject> dataPoints = new ArrayList<>();
        final HistogramFormingMetrics metrics = new HistogramFormingMetrics(7);
        for (int x = 0; x < 10000; x++) {
            GENERATOR.generate(metrics);
            final JsonObject json = new JsonObject();
            json.addProperty("min", metrics.getMin());
            json.addProperty("max", metrics.getMax());
            json.addProperty("sum", metrics.getSum());
            json.addProperty("mean", metrics.getMean());
            final JsonObject bins = new JsonObject();
            for (final Map.Entry<Double, Long> entry : metrics.getHistogram().entrySet()) {
                bins.addProperty(entry.getKey().toString(), entry.getValue());
            }
            json.add("bins", bins);
            json.addProperty("precision", metrics.getPrecision());
            dataPoints.add(json);
        }

        DATA_POINTS_JSON = dataPoints;
    }

    protected void runTest(final DataPointFactory dataPointFactory) throws IOException {
        final long timeInEpochMillis = ZonedDateTime.now().toEpochSecond() * 1000;
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final DataOutputStream outputStream = new DataOutputStream(out);

        for (final JsonObject json : DATA_POINTS_JSON) {
            final DataPoint dataPoint = dataPointFactory.getDataPoint(timeInEpochMillis, json);
            dataPoint.writeValueToBuffer(outputStream);
        }

        outputStream.close();
        out.close();

        logger.info(dataPointFactory.getClass().getSimpleName() + " total Bytes: " + out.size());
    }
}
