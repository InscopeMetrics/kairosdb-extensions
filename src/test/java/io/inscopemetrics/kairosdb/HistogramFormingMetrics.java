/*
 * Copyright 2019 Inscope Metrics, Inc
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

import com.arpnetworking.metrics.Counter;
import com.arpnetworking.metrics.Metrics;
import com.arpnetworking.metrics.Timer;
import com.google.common.collect.Maps;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Metrics implementation that pulls all data into a single histogram.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
@SuppressFBWarnings(value = "THROWS_METHOD_THROWS_RUNTIMEEXCEPTION", justification = "Test utility with stub methods")
public class HistogramFormingMetrics implements Metrics {

    private final int precision;
    private final Map<Double, Long> histogram = Maps.newHashMap();
    private Double sum = 0d;
    private Double max;
    private Double min;
    private long count;

    /**
     * Public constructor.
     *
     * @param precision precision of the bins in the histogram, in bits
     */
    public HistogramFormingMetrics(final int precision) {
        this.precision = precision;
    }

    @Override
    public Counter createCounter(final String name) {
        return null;
    }

    @Override
    public void incrementCounter(final String name) {
        throw new RuntimeException();
    }

    @Override
    public void incrementCounter(final String name, final long value) {
        throw new RuntimeException();
    }

    @Override
    public void decrementCounter(final String name) {
        throw new RuntimeException();

    }

    @Override
    public void decrementCounter(final String name, final long value) {
        throw new RuntimeException();
    }

    @Override
    public void resetCounter(final String name) {
        throw new RuntimeException();
    }

    @Override
    public Timer createTimer(final String name) {
        return null;
    }

    @Override
    public void startTimer(final String name) {
        throw new RuntimeException();

    }

    @Override
    public void stopTimer(final String name) {
        throw new RuntimeException();

    }

    @Override
    public void setTimer(final String name, final long duration, @Nullable final TimeUnit unit) {
        recordValue(duration, 1);
    }

    @Override
    public void setGauge(final String name, final double value) {
        recordValue(value, 1);
    }

    @Override
    public void setGauge(final String name, final long value) {
        throw new RuntimeException();
    }

    @Override
    public void addAnnotation(final String key, final String value) {
    }

    @Override
    public void addAnnotations(final Map<String, String> map) {
    }

    @Override
    public boolean isOpen() {
        return false;
    }

    @Override
    public void close() {
    }

    @Nullable
    @Override
    public Instant getOpenTime() {
        return null;
    }

    @Nullable
    @Override
    public Instant getCloseTime() {
        return null;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "Test utility intentionally exposes internal state")
    public Map<Double, Long> getHistogram() {
        return histogram;
    }

    public int getPrecision() {
        return precision;
    }

    public Double getSum() {
        return sum;
    }

    public Double getMax() {
        return max;
    }

    public Double getMin() {
        return min;
    }

    public Double getMean() {
        return sum / count;
    }

    private void recordValue(final double value, final long bucketCount) {
        final HistogramKeyUtility keyUtility = HistogramKeyUtility.getInstance(precision);
        histogram.merge(keyUtility.truncateToDouble(value), bucketCount, (i, j) -> i + j);
        if (min == null || value < min) {
            min = value;
        }
        if (max == null || value > max) {
            max = value;
        }
        this.count += bucketCount;
        sum += value;
    }
}
