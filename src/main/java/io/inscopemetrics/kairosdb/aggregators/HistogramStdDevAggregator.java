/*
 * Copyright 2018 Bruno Green.
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
import com.google.inject.Inject;
import io.inscopemetrics.kairosdb.HistogramDataPoint;
import io.inscopemetrics.kairosdb.accumulators.AccumulatorFactory;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.RangeAggregator;
import org.kairosdb.core.annotation.FeatureComponent;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Aggregator that computes the standard deviation value of histograms.
 *
 * @author Bruno Green (bruno dot green at gmail dot com)
 */
@FeatureComponent(
        name = "hdev",
        description = "Computes the standard deviation value of the histograms.")
public class HistogramStdDevAggregator extends RangeAggregator {
    private final DoubleDataPointFactory dataPointFactory;
    private final AccumulatorFactory accumulatorFactory;

    /**
     * Public constructor.
     *
     * @param dataPointFactory A factory for creating DoubleDataPoints
     * @param accumulatorFactory A factory for creating Accumulators
     */
    @Inject
    public HistogramStdDevAggregator(
            final DoubleDataPointFactory dataPointFactory,
            final AccumulatorFactory accumulatorFactory) {
        this.dataPointFactory = dataPointFactory;
        this.accumulatorFactory = accumulatorFactory;
    }

    @Override
    protected RangeSubAggregator getSubAggregator() {
        return new HistogramStdDevDataPointAggregator(accumulatorFactory);
    }

    @Override
    public boolean canAggregate(final String groupType) {
        return HistogramDataPoint.GROUP_TYPE.equals(groupType);
    }

    @Override
    public String getAggregatedGroupType(final String groupType) {
        return dataPointFactory.getGroupType();
    }

    private final class HistogramStdDevDataPointAggregator implements RangeSubAggregator {
        private final AccumulatorFactory accumulatorFactory;

        HistogramStdDevDataPointAggregator(final AccumulatorFactory accumulatorFactory) {
            this.accumulatorFactory = accumulatorFactory;
        }

        @Override
        public Iterable<DataPoint> getNextDataPoints(final long returnTime, final Iterator<DataPoint> dataPointRange) {
            long count = 0;
            final Accumulator meanAccumulator = accumulatorFactory.create();
            final Accumulator meanSquaredAccumulator = accumulatorFactory.create();

            while (dataPointRange.hasNext()) {
                final DataPoint dp = dataPointRange.next();
                if (dp instanceof HistogramDataPoint) {
                    final HistogramDataPoint hist = (HistogramDataPoint) dp;
                    final NavigableMap<Double, Long> map = hist.getMap();
                    if (map != null) {
                        for (final Map.Entry<Double, Long> entry : map.entrySet()) {
                            final long n = entry.getValue();
                            if (n > 0) {
                                final double x = entry.getKey();
                                count += n;
                                final double delta = x - meanAccumulator.getSum();
                                meanAccumulator.accumulate(((double) n / count) * delta);
                                meanSquaredAccumulator.accumulate(n * delta * (x - meanAccumulator.getSum()));
                            }
                        }
                    }
                }
            }

            return Collections.singletonList(
                    dataPointFactory.createDataPoint(
                            returnTime,
                            Math.sqrt(meanSquaredAccumulator.getSum() / (count - 1))));
        }
    }
}

