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

import com.google.common.collect.Maps;
import io.inscopemetrics.kairosdb.HistogramDataPoint;
import io.inscopemetrics.kairosdb.HistogramDataPointImpl;
import org.junit.Test;
import org.kairosdb.core.datapoints.DoubleDataPoint;
import org.kairosdb.core.datapoints.DoubleDataPointFactoryImpl;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.testing.ListDataPointGroup;

import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test class for {@link HistogramCountAggregator}.
 *
 * @author William Ehlardt (whale at dropbox dot com)
 */
public class HistogramCountAggregatorTest {

    @Test
    public void testCombineLargeValues() throws KairosDBException {
        final double min = Double.MAX_VALUE;
        final double max = -Double.MAX_VALUE;
        final double sum = 0;
        final TreeMap<Double, Integer> bins = Maps.newTreeMap();
        bins.put(1337d, 2147483647);
        final double mean =  1337d;

        final HistogramDataPoint dp = new HistogramDataPointImpl(
                1L, bins,  min, max, mean, sum);
        final ListDataPointGroup group = new ListDataPointGroup("testCombineLargeValues");
        group.addDataPoint(dp);
        group.addDataPoint(dp);
        group.addDataPoint(dp);
        group.addDataPoint(dp);

        final HistogramCountAggregator aggregator = new HistogramCountAggregator(new DoubleDataPointFactoryImpl());
        final DataPointGroup result = aggregator.aggregate(group);

        assertTrue(result.hasNext());
        final DoubleDataPoint resultDataPoint = (DoubleDataPoint) result.next();
        assertEquals(8589934588L, resultDataPoint.getLongValue());
    }
}
