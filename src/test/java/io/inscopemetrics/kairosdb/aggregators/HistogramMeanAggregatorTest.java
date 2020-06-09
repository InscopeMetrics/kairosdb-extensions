/*
 * Copyright 2020 Dropbox Inc.
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

import com.google.common.collect.ImmutableMap;
import io.inscopemetrics.kairosdb.accumulators.AccumulatorFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.kairosdb.core.datapoints.DoubleDataPoint;
import org.kairosdb.core.datapoints.DoubleDataPointFactoryImpl;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.testing.ListDataPointGroup;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test class for {@link HistogramMeanAggregator}.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
@RunWith(Parameterized.class)
public final class HistogramMeanAggregatorTest {

    private final AggregatorTestHelper.CreateHistogramFromValues histogramCreatorFromValues;
    private final AggregatorTestHelper.CreateHistogramFromCounts histogramCreatorFromCounts;
    private final AccumulatorFactory accumulatorFactory;

    public HistogramMeanAggregatorTest(
            final AggregatorTestHelper.CreateHistogramFromValues histogramCreatorFromValues,
            final AggregatorTestHelper.CreateHistogramFromCounts histogramCreatorFromCounts,
            final AccumulatorFactory accumulatorFactory) {
        this.histogramCreatorFromValues = histogramCreatorFromValues;
        this.histogramCreatorFromCounts = histogramCreatorFromCounts;
        this.accumulatorFactory = accumulatorFactory;
    }

    @Parameterized.Parameters(name = "{index}: {0} {2}")
    public static Collection<Object[]> parameters() {
        return AggregatorTestHelper.createParameters(AggregatorTestHelper.createAccumulatorParameterizations());
    }

    @Test
    public void testAggregator() {
        final ListDataPointGroup group = new ListDataPointGroup("HistogramMeanAggregator.testAggregator");
        group.addDataPoint(histogramCreatorFromValues.create(1L, Arrays.asList(1.0, 1.0)));
        group.addDataPoint(histogramCreatorFromValues.create(1L, Arrays.asList(2.0, 3.0)));
        group.addDataPoint(histogramCreatorFromValues.create(2L, Arrays.asList(1.0, 5.0)));
        group.addDataPoint(histogramCreatorFromValues.create(2L, Arrays.asList(4.0, 2.0)));

        final HistogramMeanAggregator aggregator = new HistogramMeanAggregator(
                new DoubleDataPointFactoryImpl(),
                accumulatorFactory);

        final DataPointGroup result = aggregator.aggregate(group);
        DoubleDataPoint resultDataPoint;
        assertTrue(result.hasNext());
        resultDataPoint = (DoubleDataPoint) result.next();
        assertEquals(1.75, resultDataPoint.getDoubleValue(), 0.0001);

        assertTrue(result.hasNext());
        resultDataPoint = (DoubleDataPoint) result.next();
        assertEquals(3.0, resultDataPoint.getDoubleValue(), 0.0001);

        assertFalse(result.hasNext());
    }

    /*
    @Test
    public void tesEmptyBinsAverage() {
        final ListDataPointGroup group = new ListDataPointGroup("HistogramMeanAggregator.tesNoDataAverage");
        group.addDataPoint(new HistogramDataPointV2Impl(0L, 7, new TreeMap<>(), 0.0, 0.0, 0.0, 0.0));

        final HistogramMeanAggregator aggregator = new HistogramMeanAggregator(
                new DoubleDataPointFactoryImpl(),
                accumulatorFactory);

        final DataPointGroup result = aggregator.aggregate(group);
        assertTrue(result.hasNext());
        final DoubleDataPoint resultDataPoint = (DoubleDataPoint) result.next();
        assertEquals(0.0, resultDataPoint.getDoubleValue(), 0.0001);

        assertFalse(result.hasNext());
    }
    */

    @Test
    public void testCountDoesNotOverflow() {
        final ListDataPointGroup group = new ListDataPointGroup("HistogramMeanAggregator.testCountDoesNotOverflow");
        for (int i = 0; i < 2200; ++i) {
            group.addDataPoint(histogramCreatorFromCounts.create(
                    1L,
                    ImmutableMap.of(1.0d, 1000000L)));
        }

        final HistogramMeanAggregator aggregator = new HistogramMeanAggregator(
                new DoubleDataPointFactoryImpl(),
                accumulatorFactory);

        final DataPointGroup result = aggregator.aggregate(group);
        assertTrue(result.hasNext());
        final DoubleDataPoint resultDataPoint = (DoubleDataPoint) result.next();
        assertEquals(1.0, resultDataPoint.getDoubleValue(), 0.0001);

        assertFalse(result.hasNext());
    }
}
