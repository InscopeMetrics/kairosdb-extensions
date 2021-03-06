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

import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.inscopemetrics.kairosdb.HistogramDataPoint;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.RangeAggregator;
import org.kairosdb.core.annotation.FeatureComponent;
import org.kairosdb.core.annotation.FeatureProperty;
import org.kairosdb.core.annotation.ValidationProperty;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;
import org.kairosdb.core.http.rest.validation.NonZero;

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
        name = "hpercentile",
        description = "Finds the percentile of the data range.")
public final class HistogramPercentileAggregator extends RangeAggregator {
    private final DoubleDataPointFactory dataPointFactory;
    @NonZero
    @FeatureProperty(
            label = "Percentile",
            description = "Data points returned will be in this percentile.",
            default_value = "0.1",
            validations =  {
                    @ValidationProperty(
                            expression = "value > 0",
                            message = "Percentile must be greater than 0."
                    ),
                    @ValidationProperty(
                            expression = "value < 1",
                            message = "Percentile must be smaller than 1."
                    )
            }
    )
    private double percentile = -1d;

    /**
     * Public constructor.
     *
     * @param dataPointFactory A factory for creating DoubleDataPoints
     */
    @Inject
    public HistogramPercentileAggregator(final DoubleDataPointFactory dataPointFactory) {
        this.dataPointFactory = dataPointFactory;
    }

    public void setPercentile(final double percentile) {
        this.percentile = percentile;
    }

    @Override
    protected RangeSubAggregator getSubAggregator() {
        return new HistogramPercentileDataPointAggregator();
    }

    @Override
    public boolean canAggregate(final String groupType) {
        return HistogramDataPoint.GROUP_TYPE.equals(groupType);
    }

    @Override
    public String getAggregatedGroupType(final String groupType) {
        return dataPointFactory.getGroupType();
    }

    private final class HistogramPercentileDataPointAggregator implements RangeSubAggregator {

        @Override
        public Iterable<DataPoint> getNextDataPoints(final long returnTime, final Iterator<DataPoint> dataPointRange) {
            final TreeMap<Double, Long> merged = Maps.newTreeMap();
            long count = 0;
            while (dataPointRange.hasNext()) {
                final DataPoint dp = dataPointRange.next();
                if (dp instanceof HistogramDataPoint) {
                    final HistogramDataPoint hist = (HistogramDataPoint) dp;
                    for (final Map.Entry<Double, Long> entry : hist.getMap().entrySet()) {
                        count += entry.getValue();
                        merged.compute(entry.getKey(), (key, existing) ->  entry.getValue() + (existing == null ? 0 : existing));
                    }
                }
            }

            final long target = (long) Math.ceil(percentile * count);
            long current = 0;
            final Iterator<Map.Entry<Double, Long>> entryIterator = merged.entrySet().iterator();
            while (entryIterator.hasNext()) {
                final Map.Entry<Double, Long> entry = entryIterator.next();
                current += entry.getValue();
                if (current >= target) {
                    return Collections.singletonList(dataPointFactory.createDataPoint(returnTime, entry.getKey()));
                }
            }
            return Collections.emptyList();
        }
    }
}
