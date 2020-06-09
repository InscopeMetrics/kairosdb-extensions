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
package io.inscopemetrics.kairosdb.aggregators;

import com.arpnetworking.commons.math.Accumulator;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.inscopemetrics.kairosdb.HistogramDataPoint;
import io.inscopemetrics.kairosdb.HistogramDataPointV2Impl;
import io.inscopemetrics.kairosdb.HistogramKeyUtility;
import io.inscopemetrics.kairosdb.accumulators.AccumulatorFactory;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.RangeAggregator;
import org.kairosdb.core.annotation.FeatureComponent;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Aggregator that computes a percentile of histograms.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
@FeatureComponent(
        name = "merge",
        description = "Merges histograms.")
public final class HistogramMergeAggregator extends RangeAggregator {

    private static final int MAX_PRECISION = 64;

    private final AccumulatorFactory accumulatorFactory;

    /**
     * Public constructor.
     *
     * @param accumulatorFactory A factory for creating Accumulators
     */
    @Inject
    public HistogramMergeAggregator(final AccumulatorFactory accumulatorFactory) {
        this.accumulatorFactory = accumulatorFactory;
    }

    @Override
    protected RangeSubAggregator getSubAggregator() {
        return new HistogramMergeDataPointAggregator(accumulatorFactory);
    }

    @Override
    public boolean canAggregate(final String groupType) {
        return HistogramDataPoint.GROUP_TYPE.equals(groupType);
    }

    @Override
    public String getAggregatedGroupType(final String groupType) {
        return HistogramDataPoint.GROUP_TYPE;
    }

    private static final class HistogramMergeDataPointAggregator implements RangeSubAggregator {

        private final AccumulatorFactory accumulatorFactory;

        HistogramMergeDataPointAggregator(final AccumulatorFactory accumulatorFactory) {
            this.accumulatorFactory = accumulatorFactory;
        }

        @Override
        public Iterable<DataPoint> getNextDataPoints(final long returnTime, final Iterator<DataPoint> dataPointRange) {
            TreeMap<Double, Long> merged = Maps.newTreeMap();
            int precision = MAX_PRECISION;
            HistogramKeyUtility keyUtility = HistogramKeyUtility.getInstance(precision);
            double min = Double.MAX_VALUE;
            double max = -Double.MAX_VALUE;
            long count = 0;
            long originalCount = 0;
            final Accumulator accumulator = accumulatorFactory.create();

            while (dataPointRange.hasNext()) {
                final DataPoint dp = dataPointRange.next();
                if (dp instanceof HistogramDataPoint) {
                    final HistogramDataPoint hist = (HistogramDataPoint) dp;

                    // If precision is less than our current precision, we need
                    // to down sample the values in the map to the lower
                    // precision.
                    if (hist.getPrecision() < precision) {
                        precision = hist.getPrecision();
                        final HistogramKeyUtility newKeyUtility = HistogramKeyUtility.getInstance(precision);
                        keyUtility = newKeyUtility;

                        final TreeMap<Double, Long> downsampled = Maps.newTreeMap();
                        for (final Map.Entry<Double, Long> entry : merged.entrySet()) {
                            final Double mergedKey = entry.getKey();
                            final Long mergedValue = entry.getValue();

                            // Since precision is decreasing multiple keys from
                            // merged may update the same bucket in downsampled
                            downsampled.merge(
                                newKeyUtility.truncateToDouble(mergedKey),
                                mergedValue,
                                Long::sum);
                        }
                        merged = downsampled;

                    }

                    for (final Map.Entry<Double, Long> entry : hist.getMap().entrySet()) {
                        // All we know is that precision is not going down but
                        // the precision of hist may be larger than merged
                        // so we need to truncate all the hist keys
                        merged.merge(
                                keyUtility.truncateToDouble(entry.getKey()),
                                entry.getValue(),
                                Long::sum);
                        count += entry.getValue();
                    }

                    originalCount += hist.getOriginalCount();

                    min = Math.min(min, hist.getMin());
                    max = Math.max(max, hist.getMax());
                    accumulator.accumulate(hist.getSum());
                }
            }

            final double sum = accumulator.getSum();
            final double mean = sum / count;

            return Collections.singletonList(
                    new HistogramDataPointV2Impl(
                            returnTime,
                            precision,
                            merged,
                            min,
                            max,
                            mean,
                            sum,
                            originalCount));
        }
    }
}
