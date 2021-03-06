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
import com.google.inject.Inject;
import io.inscopemetrics.kairosdb.HistogramDataPoint;
import io.inscopemetrics.kairosdb.accumulators.AccumulatorFactory;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.RangeAggregator;
import org.kairosdb.core.annotation.FeatureComponent;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;

import java.util.Collections;
import java.util.Iterator;

/**
 * Aggregator that computes the sum of histograms.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
@FeatureComponent(
        name = "hsum",
        description = "Adds data points together.")
public final class HistogramSumAggregator extends RangeAggregator {
    private final DoubleDataPointFactory dataPointFactory;
    private final AccumulatorFactory accumulatorFactory;

    /**
     * Public constructor.
     *
     * @param dataPointFactory A factory for creating DoubleDataPoints
     * @param accumulatorFactory A factory for creating Accumulators
     */
    @Inject
    public HistogramSumAggregator(
            final DoubleDataPointFactory dataPointFactory,
            final AccumulatorFactory accumulatorFactory) {
        this.dataPointFactory = dataPointFactory;
        this.accumulatorFactory = accumulatorFactory;
    }

    @Override
    protected RangeSubAggregator getSubAggregator() {
        return new HistogramSumDataPointAggregator();
    }

    @Override
    public boolean canAggregate(final String groupType) {
        return HistogramDataPoint.GROUP_TYPE.equals(groupType);
    }

    @Override
    public String getAggregatedGroupType(final String groupType) {
        return dataPointFactory.getGroupType();
    }

    private final class HistogramSumDataPointAggregator implements RangeSubAggregator {

        @Override
        public Iterable<DataPoint> getNextDataPoints(final long returnTime, final Iterator<DataPoint> dataPointRange) {
            final Accumulator accumulator = accumulatorFactory.create();

            while (dataPointRange.hasNext()) {
                final DataPoint dp = dataPointRange.next();
                if (dp instanceof HistogramDataPoint) {
                    final HistogramDataPoint hist = (HistogramDataPoint) dp;
                    accumulator.accumulate(hist.getSum());
                }
            }

            return Collections.singletonList(dataPointFactory.createDataPoint(returnTime, accumulator.getSum()));
        }
    }
}
