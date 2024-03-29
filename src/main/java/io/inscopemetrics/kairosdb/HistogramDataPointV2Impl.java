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

import com.google.common.base.MoreObjects;
import com.google.common.base.Suppliers;
import io.inscopemetrics.kairosdb.proto.v2.FormatV2;
import org.json.JSONException;
import org.json.JSONWriter;
import org.kairosdb.core.datapoints.DataPointHelper;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;

/**
 * DataPoint that represents a Histogram.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
public class HistogramDataPointV2Impl extends DataPointHelper implements HistogramDataPoint {
    private final int precision;
    private final TreeMap<Double, Long> map;
    private final double min;
    private final double max;
    private final double mean;
    private final double sum;
    private final Supplier<Long> originalCountSupplier;
    private final Supplier<Long> countSupplier;

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
    public HistogramDataPointV2Impl(
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
        countSupplier = Suppliers.memoize(this::computeSampleCount)::get;
        originalCountSupplier = Suppliers.memoize(countSupplier::get)::get;
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
    public HistogramDataPointV2Impl(
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
        countSupplier = Suppliers.memoize(this::computeSampleCount)::get;
        originalCountSupplier = Suppliers.ofInstance(originalCount)::get;
    }
    // CHECKSTYLE.ON: ParameterNumber

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("precision", precision)
                .add("map", map)
                .add("min", min)
                .add("max", max)
                .add("mean", mean)
                .add("sum", sum)
                .add("timestamp", m_timestamp)
                .toString();
    }

    @Override
    public void writeValueToBuffer(final DataOutput buffer) throws IOException {
        final FormatV2.DataPoint.Builder builder = FormatV2.DataPoint.newBuilder();
        final HistogramKeyUtility keyUtility = HistogramKeyUtility.getInstance(precision);

        for (final Map.Entry<Double, Long> entry : map.entrySet()) {
            builder.addBucketKey(keyUtility.pack(entry.getKey()));
            builder.addBucketCount(entry.getValue());
        }

        builder.setMax(max);
        builder.setMin(min);
        builder.setMean(mean);
        builder.setSum(sum);
        builder.setPrecision(precision);

        final byte[] bytes = builder.build().toByteArray();
        buffer.writeInt(bytes.length);
        buffer.write(bytes);
    }

    @Override
    public void writeValueToJson(final JSONWriter writer) throws JSONException {
        writer.object().key("bins");
        writer.object();
        for (final Map.Entry<Double, Long> entry : map.entrySet()) {
            writer.key(entry.getKey().toString()).value(entry.getValue());
        }
        writer.endObject();
        writer.key("min").value(Double.isFinite(min) ? min : null);
        writer.key("max").value(Double.isFinite(max) ? max : null);
        writer.key("mean").value(Double.isFinite(mean) ? mean : null);
        writer.key("sum").value(Double.isFinite(sum) ? sum : null);
        writer.key("precision").value(precision);
        writer.endObject();
    }

    @Override
    public String getApiDataType() {
        return HistogramDataPoint.GROUP_TYPE;
    }

    @Override
    public String getDataStoreDataType() {
        return HistogramDataPointV2Factory.DST;
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
    public long getSampleCount() {
        return countSupplier.get();
    }

    private long computeSampleCount() {
        long count = 0;
        for (final Long binSamples : map.values()) {
            count += binSamples;
        }
        return count;
    }

    @Override
    public long getOriginalCount() {
        return originalCountSupplier.get();
    }


    @Override
    public int getPrecision() {
        return precision;
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
