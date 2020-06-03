/*
 * Copyright 2019 Dropbox Inc.
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
package org.kairosdb.testing;

import com.google.common.collect.Maps;
import io.inscopemetrics.kairosdb.HistogramDataPoint;
import io.inscopemetrics.kairosdb.HistogramDataPointV1Impl;
import io.inscopemetrics.kairosdb.HistogramDataPointV2Impl;
import io.inscopemetrics.kairosdb.HistogramKeyUtility;
import org.junit.Assert;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datastore.DataPointGroup;

import java.util.Map;
import java.util.TreeMap;

/**
 * Utility class for creating and evaluating histograms for testing.
 *
 * @author Joey Jackson (jjackson at dropbox dot com)
 */
public final class HistogramUtils {

    // NOTE: This must match the value in HistogramDataPointV1Impl
    private static final int DEFAULT_PRECISION = 7;

    private HistogramUtils() { }

    /**
     * Checks whether two data point groups made exclusively of histogram data points are equivalent.
     *
     * @param expected the expected histogram group
     * @param actual the actual histogram group
     */
    public static void assertHistogramGroupsEqual(final DataPointGroup expected, final DataPointGroup actual) {
        while (expected.hasNext()) {
            Assert.assertTrue("Actual group is missing data points", actual.hasNext());
            final DataPoint act = actual.next();
            final DataPoint exp = expected.next();
            Assert.assertEquals("Expected and actual timestamps do not match", act.getTimestamp(),
                    exp.getTimestamp());
            assertHistogramsEqual(exp, act);
        }
        Assert.assertFalse("Actual group has too many data points", actual.hasNext());
    }

    /**
     * Checks whether two histogram data points are equivalent.
     *
     * @param expected the expected data point of type histogram data point
     * @param actual the actual data point of type histogram data point
     */
    public static void assertHistogramsEqual(final DataPoint expected, final DataPoint actual) {
        Assert.assertTrue("Data point not an instance of class HistogramDataPoint",
                expected instanceof HistogramDataPoint);
        Assert.assertTrue("Data point not an instance of class HistogramDataPoint",
                actual instanceof HistogramDataPoint);
        final HistogramDataPoint hist1 = (HistogramDataPoint) expected;
        final HistogramDataPoint hist2 = (HistogramDataPoint) actual;

        Assert.assertEquals("Histograms did not match", hist1.getMap(), hist2.getMap());
        Assert.assertEquals(hist1.getSampleCount(), hist2.getSampleCount());
        Assert.assertEquals(hist1.getSum(), hist2.getSum(), 0);
        Assert.assertEquals(hist1.getMin(), hist2.getMin(), 0);
        Assert.assertEquals(hist1.getMax(), hist2.getMax(), 0);
    }

    /**
     * Creates a data point group from an arbitrary sized list of data points.
     *
     * @param dataPoints the list of data points
     * @return the data point group
     */
    public static ListDataPointGroup createGroup(final Iterable<DataPoint> dataPoints) {
        final ListDataPointGroup group = new ListDataPointGroup("test_values");
        for (DataPoint dp : dataPoints) {
            group.addDataPoint(dp);
        }
        return group;
    }

    /**
     * Creates a V1 histogram data point with the given timestamp and given bin
     * values.
     *
     * @param timeStamp the timestamp for the histogram
     * @param actualValues the values for the histogram
     * @return the histogram data point
     */
    public static HistogramDataPoint createHistogramV1(final long timeStamp, final Iterable<Double> actualValues) {
        double min = Double.MAX_VALUE;
        double max = -Double.MAX_VALUE;
        double sum = 0;
        double count = 0;
        final TreeMap<Double, Long> bins = Maps.newTreeMap();
        final HistogramKeyUtility keyUtility = HistogramKeyUtility.getInstance(DEFAULT_PRECISION);

        for (final Double actualValue : actualValues) {
            final double binValue = keyUtility.truncateToDouble(actualValue);
            sum += actualValue;
            min = Math.min(min, actualValue);
            max = Math.max(max, actualValue);
            count++;
            bins.compute(binValue, (key, value) -> value == null ? 1 : value + 1);
        }
        final double mean = sum / count;

        return new HistogramDataPointV1Impl(timeStamp, bins, min, max, mean, sum);
    }

    /**
     * Creates a V1 histogram data point with the given timestamp and given bin
     * value counts.
     *
     * @param timeStamp the timestamp for the histogram
     * @param valueCounts the value (not truncated) to count
     * @return the histogram data point
     */
    public static HistogramDataPoint createHistogramV1(final long timeStamp, final Map<Double, Long> valueCounts) {
        double min = Double.MAX_VALUE;
        double max = -Double.MAX_VALUE;
        double sum = 0;
        double count = 0;
        final TreeMap<Double, Long> bins = Maps.newTreeMap();
        final HistogramKeyUtility keyUtility = HistogramKeyUtility.getInstance(DEFAULT_PRECISION);

        for (final Map.Entry<Double, Long> valueCount : valueCounts.entrySet()) {
            final double actualValue = valueCount.getKey();
            final double binValue = keyUtility.truncateToDouble(actualValue);
            final long binCount = valueCount.getValue();
            sum += binCount * actualValue;
            min = Math.min(min, actualValue);
            max = Math.max(max, actualValue);
            count += binCount;
            bins.compute(binValue, (key, value) -> value == null ? binCount : value + binCount);
        }
        final double mean = sum / count;

        return new HistogramDataPointV1Impl(timeStamp, bins, min, max, mean, sum);
    }

    /**
     * Creates a V2 histogram data point with the given timestamp and given bin
     * values.
     *
     * @param timeStamp the timestamp for the histogram
     * @param binValues the bin vales for the histogram
     * @return the histogram data point
     */
    public static HistogramDataPoint createHistogramV2(final long timeStamp, final Iterable<Double> binValues) {
        return createHistogramV2(timeStamp, DEFAULT_PRECISION, binValues);
    }

    /**
     * Creates a V2 histogram data point with the given timestamp and given bin
     * values.
     *
     * @param timeStamp the timestamp for the histogram
     * @param precision the precision for the histogram
     * @param actualValues the values for the histogram
     * @return the histogram data point
     */
    public static HistogramDataPoint createHistogramV2(
            final long timeStamp,
            final int precision,
            final Iterable<Double> actualValues) {
        double min = Double.MAX_VALUE;
        double max = -Double.MAX_VALUE;
        double sum = 0;
        double count = 0;
        final TreeMap<Double, Long> bins = Maps.newTreeMap();
        final HistogramKeyUtility keyUtility = HistogramKeyUtility.getInstance(precision);

        for (final Double actualValue : actualValues) {
            final double binValue = keyUtility.truncateToDouble(actualValue);
            sum += actualValue;
            min = Math.min(min, actualValue);
            max = Math.max(max, actualValue);
            count++;
            bins.compute(binValue, (key, value) -> value == null ? 1 : value + 1);
        }
        final double mean = sum / count;

        return new HistogramDataPointV2Impl(timeStamp, precision, bins, min, max, mean, sum);
    }

    /**
     * Creates a V2 histogram data point with the given timestamp and given bin
     * value counts.
     *
     * @param timeStamp the timestamp for the histogram
     * @param valueCounts the value (not truncated) to count
     * @return the histogram data point
     */
    public static HistogramDataPoint createHistogramV2(
            final long timeStamp,
            final Map<Double, Long> valueCounts) {
        return createHistogramV2(timeStamp, DEFAULT_PRECISION, valueCounts);
    }

    /**
     * Creates a V2 histogram data point with the given timestamp and given bin
     * value counts.
     *
     * @param timeStamp the timestamp for the histogram
     * @param precision the precision for the histogram
     * @param valueCounts the value (not truncated) to count
     * @return the histogram data point
     */
    public static HistogramDataPoint createHistogramV2(
            final long timeStamp,
            final int precision,
            final Map<Double, Long> valueCounts) {
        double min = Double.MAX_VALUE;
        double max = -Double.MAX_VALUE;
        double sum = 0;
        double count = 0;
        final TreeMap<Double, Long> bins = Maps.newTreeMap();
        final HistogramKeyUtility keyUtility = HistogramKeyUtility.getInstance(precision);

        for (final Map.Entry<Double, Long> valueCount : valueCounts.entrySet()) {
            final double actualValue = valueCount.getKey();
            final double binValue = keyUtility.truncateToDouble(actualValue);
            final long binCount = valueCount.getValue();
            sum += binCount * actualValue;
            min = Math.min(min, actualValue);
            max = Math.max(max, actualValue);
            count += binCount;
            bins.compute(binValue, (key, value) -> value == null ? binCount : value + binCount);
        }
        final double mean = sum / count;

        return new HistogramDataPointV2Impl(timeStamp, precision, bins, min, max, mean, sum);
    }
}
