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
 * Test class for {@link HistogramPercentileAggregator}.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
@RunWith(Parameterized.class)
public final class HistogramPercentileAggregatorTest extends AbstractHistogramTest {

    private final CreateHistogramFromValues histogramCreatorFromValues;

    public HistogramPercentileAggregatorTest(final CreateHistogramFromValues histogramCreatorFromValues) {
        this.histogramCreatorFromValues = histogramCreatorFromValues;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        return createParametersFromValues();
    }

    @Test
    public void testAggregator() {
        final HistogramPercentileAggregator aggregator = new HistogramPercentileAggregator(new DoubleDataPointFactoryImpl());
        DataPointGroup result;
        DoubleDataPoint resultDataPoint;

        aggregator.setPercentile(0.9);
        result = aggregator.aggregate(createDataPointGroup());
        assertTrue(result.hasNext());
        resultDataPoint = (DoubleDataPoint) result.next();
        assertEquals(5.0, resultDataPoint.getDoubleValue(), 0.0001);

        aggregator.setPercentile(0.7);
        result = aggregator.aggregate(createDataPointGroup());
        assertTrue(result.hasNext());
        resultDataPoint = (DoubleDataPoint) result.next();
        assertEquals(3.0, resultDataPoint.getDoubleValue(), 0.0001);

        aggregator.setPercentile(0.2);
        result = aggregator.aggregate(createDataPointGroup());
        assertTrue(result.hasNext());
        resultDataPoint = (DoubleDataPoint) result.next();
        assertEquals(1.0, resultDataPoint.getDoubleValue(), 0.0001);

        assertFalse(result.hasNext());
    }

    private ListDataPointGroup createDataPointGroup() {
        // Value: 1, 1, 1, 2, 2, 3, 3, 4, 5, 5
        // Index: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

        final ListDataPointGroup group = new ListDataPointGroup("HistogramPercentileAggregator");
        group.addDataPoint(histogramCreatorFromValues.create(1L, Arrays.asList(1.0, 1.0)));
        group.addDataPoint(histogramCreatorFromValues.create(1L, Arrays.asList(2.0, 3.0)));
        group.addDataPoint(histogramCreatorFromValues.create(1L, Arrays.asList(1.0, 5.0)));
        group.addDataPoint(histogramCreatorFromValues.create(1L, Arrays.asList(4.0, 2.0)));
        group.addDataPoint(histogramCreatorFromValues.create(1L, Arrays.asList(3.0, 5.0)));
        return group;
    }
}
