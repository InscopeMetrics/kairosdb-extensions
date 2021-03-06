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
package io.inscopemetrics.kairosdb;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.json.JSONException;
import org.json.JSONWriter;
import org.kairosdb.core.datapoints.DataPointHelper;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * DataPoint that represents a V1 Histogram.
 *
 * <b>VERSION NOTE:</b> This {@link HistogramDataPoint} is an implementation for
 * the v1 factory {@link HistogramDataPointFactory}. The factory could not be
 * renamed to v1 because it's fully qualified class name is referenced by
 * KairosDb config.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
public class HistogramDataPointV1Impl extends DataPointHelper implements HistogramDataPoint {
    private static final int DEFAULT_PRECISION = 7;

    @SuppressFBWarnings("URF_UNREAD_FIELD")
    private final int precision;
    private final TreeMap<Double, Long> map;
    private final double min;
    private final double max;
    private final double mean;
    private final double sum;
    private final long originalCount;

    /**
     * Public constructor.
     *
     * @param timestamp the timestamp.
     * @param map the bins with values
     * @param min the minimum value in the histogram
     * @param max the maximum value in the histogram
     * @param mean the mean value in the histogram
     * @param sum the sum of all the values in the histogram
     */
    public HistogramDataPointV1Impl(
            final long timestamp,
            final TreeMap<Double, Long> map,
            final double min,
            final double max,
            final double mean,
            final double sum) {
        this(
                timestamp,
                DEFAULT_PRECISION,
                map,
                min,
                max,
                mean,
                sum);
    }

    /**
     * Public constructor.
     *
     * @param timestamp the timestamp.
     * @param map the bins with values
     * @param min the minimum value in the histogram
     * @param max the maximum value in the histogram
     * @param mean the mean value in the histogram
     * @param sum the sum of all the values in the histogram
     * @param originalCount the original number of data points that this histogram represented
     */
    public HistogramDataPointV1Impl(
            final long timestamp,
            final TreeMap<Double, Long> map,
            final double min,
            final double max,
            final double mean,
            final double sum,
            final long originalCount) {
        this(
                timestamp,
                DEFAULT_PRECISION,
                map,
                min,
                max,
                mean,
                sum,
                originalCount);
    }

    /**
     * Public constructor.
     *
     * @param timestamp the timestamp.
     * @param precision bucket precision, in bits
     * @param map the bins with values
     * @param min the minimum value in the histogram
     * @param max the maximum value in the histogram
     * @param mean the mean value in the histogram
     * @param sum the sum of all the values in the histogram
     */
    public HistogramDataPointV1Impl(
            final long timestamp,
            final int precision,
            final TreeMap<Double, Long> map,
            final double min,
            final double max,
            final double mean,
            final double sum) {
        super(timestamp);
        this.precision = precision;
        this.map = map;
        this.min = min;
        this.max = max;
        this.mean = mean;
        this.sum = sum;
        this.originalCount = getSampleCount();
    }


    /**
     * Public constructor.
     *
     * @param timestamp the timestamp.
     * @param precision bucket precision, in bits
     * @param map the bins with values
     * @param min the minimum value in the histogram
     * @param max the maximum value in the histogram
     * @param mean the mean value in the histogram
     * @param sum the sum of all the values in the histogram
     * @param originalCount the original number of data points that this histogram represented
     */
    // CHECKSTYLE.OFF: ParameterNumber
    public HistogramDataPointV1Impl(
            final long timestamp,
            final int precision,
            final TreeMap<Double, Long> map,
            final double min,
            final double max,
            final double mean,
            final double sum,
            final long originalCount) {
        super(timestamp);
        this.precision = precision;
        this.map = map;
        this.min = min;
        this.max = max;
        this.mean = mean;
        this.sum = sum;
        this.originalCount = originalCount;
    }
    // CHECKSTYLE.ON: ParameterNumber

    @Override
    public void writeValueToBuffer(final DataOutput buffer) throws IOException {
        buffer.writeInt(map.size());
        for (final Map.Entry<Double, Long> entry : map.entrySet()) {
            // NOTE: That the v1 format persists bucket counts as integers for
            // backwards compatibility. If your bucket counts require a long
            // you must use the v2 format.
            buffer.writeDouble(entry.getKey());
            buffer.writeInt(entry.getValue().intValue());
        }
        buffer.writeDouble(min);
        buffer.writeDouble(max);
        buffer.writeDouble(mean);
        buffer.writeDouble(sum);
    }

    @Override
    public void writeValueToJson(final JSONWriter writer) throws JSONException {
        writer.object().key("bins");
        writer.object();
        for (final Map.Entry<Double, Long> entry : map.entrySet()) {
            writer.key(entry.getKey().toString()).value(entry.getValue());
        }
        writer.endObject();
        writer.key("min").value(min);
        writer.key("max").value(max);
        writer.key("mean").value(mean);
        writer.key("sum").value(sum);
        writer.endObject();
    }

    @Override
    public String getApiDataType() {
        return HistogramDataPoint.GROUP_TYPE;
    }

    @Override
    public String getDataStoreDataType() {
        return HistogramDataPointFactory.DST;
    }

    @Override
    public boolean isLong() {
        return false;
    }

    @Override
    public long getLongValue() {
        return 0;
    }

    @Override
    public boolean isDouble() {
        return false;
    }

    @Override
    public double getDoubleValue() {
        return 0;
    }

    @Override
    public int getPrecision() {
        return precision;
    }

    @Override
    public long getOriginalCount() {
        return originalCount;
    }

    /**
     * Gets the number of samples in the bins.
     *
     * @return the number of samples
     */
    @Override
    public long getSampleCount() {
        long count = 0;
        for (final Long binSamples : map.values()) {
            count += binSamples;
        }
        return count;
    }

    @Override
    public double getSum() {
        return sum;
    }

    @Override
    public double getMin() {
        return min;
    }

    @Override
    public double getMax() {
        return max;
    }

    @Override
    public TreeMap<Double, Long> getMap() {
        return map;
    }
}
