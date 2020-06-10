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
import io.inscopemetrics.kairosdb.accumulators.AccumulatorFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.testing.HistogramUtils;
import org.kairosdb.testing.ListDataPointGroup;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test class for {@link HistogramMergeAggregator} variable precision logic
 * which applies only to {@link io.inscopemetrics.kairosdb.HistogramDataPointV2Impl}.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
@RunWith(Parameterized.class)
public final class HistogramMergeAggregatorVariablePrecisionTest {

    private final AccumulatorFactory accumulatorFactory;
    private final HistogramKeyUtility keyUtility = HistogramKeyUtility.getInstance(4);
    private final ListDataPointGroup group = new ListDataPointGroup("VariablePrecisionTest");

    public HistogramMergeAggregatorVariablePrecisionTest(final AccumulatorFactory accumulatorFactory) {
        this.accumulatorFactory = accumulatorFactory;

        // All values above are represented as-is at precision 7 but truncate
        // to the following at precision 4:
        //
        // 2000 -> 1984
        // 1750 -> 1728
        // 1500 -> 1472
        // 1000 -> 992

        group.addDataPoint(HistogramUtils.createHistogramV2(1L, (byte) 7, Arrays.asList(2000.0, 1000.0)));
        group.addDataPoint(HistogramUtils.createHistogramV2(1L, (byte) 7, Arrays.asList(1500.0, 1750.0)));
        group.addDataPoint(HistogramUtils.createHistogramV2(1L, (byte) 4, Arrays.asList(1995.0, 995.0)));
        group.addDataPoint(HistogramUtils.createHistogramV2(1L, (byte) 4, Arrays.asList(1475.0, 1735.0)));
    }

    @Parameterized.Parameters(name = "{index}: {0}")
    public static Collection<Object[]> parameters() {
        return AggregatorTestHelper.createAccumulatorParameterizations();
    }

    @Test
    public void testHigherToLower() {
        final HistogramMergeAggregator aggregator = new HistogramMergeAggregator(accumulatorFactory);

        final DataPointGroup result = aggregator.aggregate(group);
        assertTrue(result.hasNext());
        assertResult((HistogramDataPoint) result.next());
        assertFalse(result.hasNext());
    }

    @Test
    public void testLowerToHigher() {
        final HistogramMergeAggregator aggregator = new HistogramMergeAggregator(accumulatorFactory);

        final DataPointGroup result = aggregator.aggregate(group);
        assertTrue(result.hasNext());
        assertResult((HistogramDataPoint) result.next());
        assertFalse(result.hasNext());
    }

    private void assertResult(final HistogramDataPoint resultDataPoint) {
        assertEquals(2000.0, resultDataPoint.getMax(), 0.0001);
        assertEquals(995.0, resultDataPoint.getMin(), 0.0001);
        assertEquals(12450.0, resultDataPoint.getSum(), 0.0001);
        assertEquals(8, resultDataPoint.getSampleCount());
        assertEquals(8, resultDataPoint.getOriginalCount());
        assertEquals(4, resultDataPoint.getPrecision());
        assertEquals(4, resultDataPoint.getMap().size());
        assertEquals(2L, resultDataPoint.getMap().get(keyUtility.truncateToDouble(2000.0)).longValue());
        assertEquals(2L, resultDataPoint.getMap().get(keyUtility.truncateToDouble(1750.0)).longValue());
        assertEquals(2L, resultDataPoint.getMap().get(keyUtility.truncateToDouble(1500.0)).longValue());
        assertEquals(2L, resultDataPoint.getMap().get(keyUtility.truncateToDouble(1000.0)).longValue());
    }
}
