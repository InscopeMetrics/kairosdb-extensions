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
 * Test class for {@link HistogramStdDevAggregator}.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
@RunWith(Parameterized.class)
public final class HistogramStdDevAggregatorTest {

    private final AggregatorTestHelper.CreateHistogramFromValues histogramCreatorFromValues;
    private final AccumulatorFactory accumulatorFactory;

    public HistogramStdDevAggregatorTest(
            final AggregatorTestHelper.CreateHistogramFromValues histogramCreatorFromValues,
            final AccumulatorFactory accumulatorFactory) {
        this.histogramCreatorFromValues = histogramCreatorFromValues;
        this.accumulatorFactory = accumulatorFactory;
    }

    @Parameterized.Parameters(name = "{index}: {0} {1}")
    public static Collection<Object[]> parameters() {
        return AggregatorTestHelper.createParametersFromValues(AggregatorTestHelper.createAccumulatorParameterizations());
    }

    @Test
    public void testAggregator() {
        // Example data from:
        // https://en.wikipedia.org/wiki/Standard_deviation
        final ListDataPointGroup group = new ListDataPointGroup("HistogramStdDevAggregator");
        group.addDataPoint(histogramCreatorFromValues.create(1L, Arrays.asList(727.7, 1086.5)));
        group.addDataPoint(histogramCreatorFromValues.create(1L, Arrays.asList(1091.0, 1361.3)));
        group.addDataPoint(histogramCreatorFromValues.create(1L, Arrays.asList(1490.5, 1956.1)));

        final HistogramStdDevAggregator aggregator = new HistogramStdDevAggregator(
                new DoubleDataPointFactoryImpl(),
                accumulatorFactory);

        final DataPointGroup result = aggregator.aggregate(group);
        assertTrue(result.hasNext());
        final DoubleDataPoint resultDataPoint = (DoubleDataPoint) result.next();
        assertEquals(420.96, resultDataPoint.getDoubleValue(), 1.0);
        assertFalse(result.hasNext());
    }
}
