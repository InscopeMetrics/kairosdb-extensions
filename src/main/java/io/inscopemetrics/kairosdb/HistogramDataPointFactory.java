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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datapoints.DataPointFactory;
import org.kairosdb.util.KDataInput;

import java.io.DataInput;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Factory that creates {@link HistogramDataPointImpl}.
 *
 * @author Brandon Arp (brandon dot arp at smartsheet dot com)
 */
public class HistogramDataPointFactory implements DataPointFactory {
    /**
     * Name of the Data Store Type.
     */
    public static final String DST = "kairos_histogram_v1";
    /**
     * Name of the group type.
     */
    public static final String GROUP_TYPE = "histogram";

    @Override
    public String getDataStoreType() {
        return DST;
    }

    @Override
    public String getGroupType() {
        return GROUP_TYPE;
    }

    @Override
    public DataPoint getDataPoint(final long timestamp, final JsonElement json) throws IOException {
        final TreeMap<Double, Integer> binValues = new TreeMap<>();

        final JsonObject object = json.getAsJsonObject();
        final double min = getFiniteDouble(object, "min");
        final double max = getFiniteDouble(object, "max");
        final double mean = getFiniteDouble(object, "mean");
        final double sum = getFiniteDouble(object, "sum");
        final JsonObject bins = object.get("bins").getAsJsonObject();

        for (Map.Entry<String, JsonElement> entry : bins.entrySet()) {
            binValues.put(
                    ensureFinite(Double.parseDouble(entry.getKey()), "bucket"),
                    entry.getValue().getAsInt()
            );
        }

        return new HistogramDataPointImpl(timestamp, binValues, min, max, mean, sum);
    }

    @Override
    public DataPoint getDataPoint(final long timestamp, final DataInput buffer) throws IOException {
        final TreeMap<Double, Integer> bins = new TreeMap<>();
        final int binCount = buffer.readInt();
        for (int i = 0; i < binCount; i++) {
            bins.put(
                    ensureFinite(buffer.readDouble(), "bucket"),
                    buffer.readInt()
            );
        }

        final double min = ensureFinite(buffer.readDouble(), "min");
        final double max = ensureFinite(buffer.readDouble(), "max");
        final double mean = ensureFinite(buffer.readDouble(), "mean");
        final double sum = ensureFinite(buffer.readDouble(), "sum");

        return new HistogramDataPointImpl(timestamp, bins, min, max, mean, sum);
    }

    @SuppressFBWarnings("BC_UNCONFIRMED_CAST") // In kdb 1.3.0+, KDataInput extends DataInput, don't call this in 1.2.x
    public DataPoint getDataPoint(final long timestamp, final KDataInput buffer) throws IOException {
        return getDataPoint(timestamp, (DataInput) buffer);
    }

    private double ensureFinite(final double x, final String name) {
        if (!Double.isFinite(x)) {
            throw new IllegalArgumentException(String.format("%s has non-finite value %s", name, Double.toString(x)));
        }
        return x;
    }

    private double getFiniteDouble(final JsonObject object, final String key) {
        return ensureFinite(object.get(key).getAsDouble(), key);
    }
}
