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

import io.inscopemetrics.kairosdb.DelegatingRangeAggregatorMap;
import org.joda.time.DateTimeZone;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.RangeAggregator;
import org.kairosdb.core.datapoints.DoubleDataPointFactoryImpl;
import org.kairosdb.core.datastore.DataPointGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import javax.inject.Provider;

/**
 * Serves as a base for an aggregator that will delegate and intelligently
 * dispatch aggregation to appropriate classes.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
public class DelegatingRangeAggregator extends RangeAggregator {
    private static final Logger LOGGER = LoggerFactory.getLogger(DelegatingRangeAggregator.class);

    private final DelegatingRangeAggregatorMap aggregatorMap;
    private boolean sampleAlign;
    private long startTime;
    private DateTimeZone timeZone = DateTimeZone.UTC;

    /**
     * Public constructor.
     *
     * @param aggregatorMap aggregators to use
     */
    public DelegatingRangeAggregator(final DelegatingRangeAggregatorMap aggregatorMap) {
        this.aggregatorMap = aggregatorMap;
    }

    @Override
    public void setAlignSampling(final boolean align) {
        sampleAlign = align;
        super.setAlignSampling(align);
    }

    @Override
    public void setStartTime(final long startTime) {
        this.startTime = startTime;
        super.setStartTime(startTime);
    }

    @Override
    public void setTimeZone(final DateTimeZone timeZone) {
        this.timeZone = timeZone;
        super.setTimeZone(timeZone);
    }

    @Override
    public DataPointGroup aggregate(final DataPointGroup dataPointGroup) {
        final PeekableDataPointGroup wrapped = new PeekableDataPointGroup(dataPointGroup);

        final String dataType;
        if (wrapped.hasNext()) {
            final DataPoint point = wrapped.peek();
            dataType = point.getDataStoreDataType();
        } else {
            dataType = DoubleDataPointFactoryImpl.DST_DOUBLE;
        }

        final Optional<RangeAggregator> aggregatorOptional = aggregatorMap.aggregatorForDataStoreDataType(dataType);
        if (!aggregatorOptional.isPresent()) {
            throw new IllegalArgumentException("Cannot aggregate a " + dataType);
        }

        final RangeAggregator aggregator = aggregatorOptional.get();
        LOGGER.trace("Delegating to a " + aggregator.getClass().getSimpleName());
        aggregator.setAlignSampling(sampleAlign);
        aggregator.setStartTime(startTime);
        aggregator.setAlignStartTime(m_alignStartTime);
        aggregator.setTimeZone(timeZone);
        aggregator.setSampling(m_sampling);

        setProperties(aggregator);
        return aggregator.aggregate(wrapped);
    }

    /**
     * Provides a way to set additional properties on the delegated aggregator.
     *
     * @param aggregator the delegated aggregator
     */
    protected void setProperties(final RangeAggregator aggregator) {
    }

    @Override
    protected RangeSubAggregator getSubAggregator() {
        throw new UnsupportedOperationException("Delegating aggregators do not provide a subAggregator");
    }

    @Override
    public boolean canAggregate(final String groupType) {
        return aggregatorMap.aggregatorForGroupType(groupType).isPresent();
    }

    @Override
    public String getAggregatedGroupType(final String groupType) {
        final Optional<Provider<? extends RangeAggregator>> provider = aggregatorMap.aggregatorForGroupType(groupType);
        if (provider.isPresent()) {
            return provider.get().get().getAggregatedGroupType(groupType);
        }
        throw new IllegalArgumentException("Cannot aggregate a " + groupType);
    }
}
