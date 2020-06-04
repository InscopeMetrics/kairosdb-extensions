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

import io.inscopemetrics.kairosdb.HistogramDataPoint;
import io.inscopemetrics.kairosdb.HistogramKeyUtility;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.testing.ListDataPointGroup;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test class for {@link HistogramMergeAggregator}.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
@RunWith(Parameterized.class)
public final class HistogramMergeAggregatorTest extends AbstractHistogramTest {

    private final CreateHistogramFromValues histogramCreatorFromValues;

    public HistogramMergeAggregatorTest(final CreateHistogramFromValues histogramCreatorFromValues) {
        this.histogramCreatorFromValues = histogramCreatorFromValues;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        return createParametersFromValues();
    }

    @Test
    public void testAggregator() {
        final HistogramKeyUtility keyUtility = HistogramKeyUtility.getInstance(7);
        final ListDataPointGroup group = new ListDataPointGroup("HistogramMergeAggregatorTest");
        group.addDataPoint(histogramCreatorFromValues.create(1L, Arrays.asList(1.0, 1.0)));
        group.addDataPoint(histogramCreatorFromValues.create(1L, Arrays.asList(2.0, 3.0)));
        group.addDataPoint(histogramCreatorFromValues.create(1L, Arrays.asList(1.0, 5.0)));
        group.addDataPoint(histogramCreatorFromValues.create(1L, Arrays.asList(4.0, 2.0)));
        group.addDataPoint(histogramCreatorFromValues.create(1L, Arrays.asList(3.0, 5.0)));

        final HistogramMergeAggregator aggregator = new HistogramMergeAggregator();

        final DataPointGroup result = aggregator.aggregate(group);
        assertTrue(result.hasNext());
        final HistogramDataPoint resultDataPoint = (HistogramDataPoint) result.next();
        assertEquals(5.0, resultDataPoint.getMax(), 0.0001);
        assertEquals(1.0, resultDataPoint.getMin(), 0.0001);
        assertEquals(27.0, resultDataPoint.getSum(), 0.0001);
        assertEquals(10, resultDataPoint.getSampleCount());
        assertEquals(10, resultDataPoint.getOriginalCount());
        assertEquals(7, resultDataPoint.getPrecision());
        assertEquals(5, resultDataPoint.getMap().size());
        assertEquals(3L, resultDataPoint.getMap().get(keyUtility.truncateToDouble(1.0)).longValue());
        assertEquals(2L, resultDataPoint.getMap().get(keyUtility.truncateToDouble(2.0)).longValue());
        assertEquals(2L, resultDataPoint.getMap().get(keyUtility.truncateToDouble(3.0)).longValue());
        assertEquals(1L, resultDataPoint.getMap().get(keyUtility.truncateToDouble(4.0)).longValue());
        assertEquals(2L, resultDataPoint.getMap().get(keyUtility.truncateToDouble(5.0)).longValue());
        assertFalse(result.hasNext());
    }
}
