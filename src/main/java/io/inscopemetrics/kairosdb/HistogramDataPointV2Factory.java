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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.inscopemetrics.kairosdb.proto.v2.FormatV2;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datapoints.DataPointFactory;

import java.io.DataInput;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Factory that creates {@link HistogramDataPointV2Impl}.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
public class HistogramDataPointV2Factory implements DataPointFactory {
    /**
     * Name of the Data Store Type.
     */
    public static final String DST = "kairos_histogram_v2";
    /**
     * Name of the group type.
     */
    public static final String GROUP_TYPE = "histogram";

    /**
     * Default constructor.
     */
    public HistogramDataPointV2Factory() { }

    @Override
    public String getDataStoreType() {
        return DST;
    }

    @Override
    public String getGroupType() {
        return GROUP_TYPE;
    }

    @Override
    public DataPoint getDataPoint(final long timestamp, final JsonElement json) {
        final TreeMap<Double, Long> binValues = new TreeMap<>();

        final JsonObject object = json.getAsJsonObject();
        final double min = object.get("min").getAsDouble();
        final double max = object.get("max").getAsDouble();
        final double mean = object.get("mean").getAsDouble();
        final double sum = object.get("sum").getAsDouble();
        final JsonObject bins = object.get("bins").getAsJsonObject();

        for (final Map.Entry<String, JsonElement> entry : bins.entrySet()) {
            binValues.put(Double.parseDouble(entry.getKey()), entry.getValue().getAsLong());
        }

        final byte precision = Optional.ofNullable(object.get("precision")).map(JsonElement::getAsByte).orElse((byte) 7);
        return new HistogramDataPointV2Impl(timestamp, precision, binValues, min, max, mean, sum);
    }

    @Override
    public DataPoint getDataPoint(final long timestamp, final DataInput buffer) throws IOException {
        final int length = buffer.readInt();
        final byte[] data = new byte[length];
        buffer.readFully(data, 0, data.length);

        final FormatV2.DataPoint protoData = FormatV2.DataPoint.parseFrom(data);
        final int precision = protoData.getPrecision();
        final HistogramKeyUtility keyUtility = HistogramKeyUtility.getInstance(precision);

        if (protoData.getBucketCountCount() != protoData.getBucketKeyCount()) {
            throw new IllegalArgumentException("Mismatched bucket keys and counts");
        }

        final TreeMap<Double, Long> bins = new TreeMap<>();
        for (int i = 0; i < protoData.getBucketCountCount(); ++i) {
            bins.put(keyUtility.unpack(protoData.getBucketKey(i)), protoData.getBucketCount(i));
        }

        final double min = protoData.getMin();
        final double max = protoData.getMax();
        final double mean = protoData.getMean();
        final double sum = protoData.getSum();

        return new HistogramDataPointV2Impl(timestamp, precision, bins, min, max, mean, sum);
    }
}
