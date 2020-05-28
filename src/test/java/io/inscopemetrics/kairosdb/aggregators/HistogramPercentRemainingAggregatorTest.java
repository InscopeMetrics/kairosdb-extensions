/*
 * Copyright 2019 Dropbox Inc.
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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.FilterAggregator;
import org.kairosdb.core.aggregator.Sampling;
import org.kairosdb.core.datapoints.DoubleDataPoint;
import org.kairosdb.core.datapoints.DoubleDataPointFactoryImpl;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.TimeUnit;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.plugin.Aggregator;
import org.kairosdb.testing.HistogramUtils;
import org.kairosdb.testing.ListDataPointGroup;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for the Histogram Percent Remaining Aggregator.
 *
 * @author Joey Jackson (jjackson at dropbox dot com)
 */
@RunWith(Parameterized.class)
public final class HistogramPercentRemainingAggregatorTest extends AbstractHistogramTest {

    private final List<DataPoint> singleHistGroup;
    private final List<DataPoint> singleDoubleGroup;
    private final List<DataPoint> emptyHistGroup;
    private final List<DataPoint> multiHistGroup;

    private HistogramPercentRemainingAggregator percentRemainingAggregator;
    private HistogramFilterAggregator filterAggregator;
    private HistogramMergeAggregator mergeAggregator;

    public HistogramPercentRemainingAggregatorTest(final CreateHistogramFromValues histogramCreator) {
        singleHistGroup = Collections.singletonList(histogramCreator.create(1L, Arrays.asList(1d, 10d, 100d, 1000d)));
        singleDoubleGroup = Collections.singletonList(new DoubleDataPoint(1L, 0d));
        emptyHistGroup = Collections.singletonList(histogramCreator.create(1L, Collections.emptyList()));
        multiHistGroup = Arrays.asList(
                histogramCreator.create(1L, Arrays.asList(10d, 20d, 30d, 40d)),
                histogramCreator.create(2L, Arrays.asList(20d, 30d, 40d, 50d)),
                histogramCreator.create(3L, Arrays.asList(30d, 40d, 50d, 60d)));
    }

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        return createParametersForHistogramFromValues(Collections.emptyList());
    }

    @Before
    public void setUp() throws KairosDBException {
        percentRemainingAggregator = new HistogramPercentRemainingAggregator(new DoubleDataPointFactoryImpl());

        filterAggregator = new HistogramFilterAggregator();
        filterAggregator.setFilterIndeterminateInclusion(HistogramFilterAggregator.FilterIndeterminate.DISCARD);

        mergeAggregator = new HistogramMergeAggregator();
        mergeAggregator.setStartTime(1L);
        mergeAggregator.setSampling(new Sampling(10, TimeUnit.MINUTES));
    }

    @Test(expected = NullPointerException.class)
    public void testPercentRemainingNull() {
        percentRemainingAggregator.aggregate(null);
    }

    @Test
    public void testPercentRemainingEmptyGroup() {
        runPercentRemainingTest(emptyHistGroup, createDoubleGroup(-1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPercentRemainingNotHistograms() {
        final DataPointGroup percentRemainingGroup = percentRemainingAggregator.aggregate(
                HistogramUtils.createGroup(singleDoubleGroup));
        assertTrue(percentRemainingGroup.hasNext());
        percentRemainingGroup.next();
    }

    @Test
    public void testPercentRemainingFilterAllOut() {
        configureFilter(FilterAggregator.FilterOperation.GT, 0d);
        runPercentRemainingTest(singleHistGroup, createDoubleGroup(), filterAggregator);
    }

    @Test
    public void testPercentRemainingFilterNoneOut() {
        configureFilter(FilterAggregator.FilterOperation.GT, 10000d);
        runPercentRemainingTest(singleHistGroup, createDoubleGroup(1d), filterAggregator);
    }

    @Test
    public void testPercentRemainingFilterLT() {
        configureFilter(FilterAggregator.FilterOperation.LT, 5d);
        runPercentRemainingTest(singleHistGroup, createDoubleGroup(0.75), filterAggregator);
    }

    @Test
    public void testPercentRemainingFilterLTE() {
        configureFilter(FilterAggregator.FilterOperation.LTE, 10d);
        runPercentRemainingTest(singleHistGroup, createDoubleGroup(0.5), filterAggregator);
    }

    @Test
    public void testPercentRemainingFilterGT() {
        configureFilter(FilterAggregator.FilterOperation.GT, 5d);
        runPercentRemainingTest(singleHistGroup, createDoubleGroup(0.25), filterAggregator);
    }

    @Test
    public void testPercentRemainingFilterGTE() {
        configureFilter(FilterAggregator.FilterOperation.GTE, 100d);
        runPercentRemainingTest(singleHistGroup, createDoubleGroup(0.5), filterAggregator);
    }

    @Test
    public void testPercentRemainingFilterEqual() {
        configureFilter(FilterAggregator.FilterOperation.EQUAL, 100d);
        runPercentRemainingTest(singleHistGroup, createDoubleGroup(0.75), filterAggregator);
    }

    @Test
    public void testPercentRemainingMerge() {
        runPercentRemainingTest(multiHistGroup, createDoubleGroup(1d), mergeAggregator);
    }

    @Test
    public void testPercentRemainingMergeThenFilter() {
        configureFilter(FilterAggregator.FilterOperation.LTE, 30d);
        runPercentRemainingTest(multiHistGroup, createDoubleGroup(0.5), mergeAggregator, filterAggregator);
    }

    @Test
    public void testPercentRemainingFilterThenMerge() {
        configureFilter(FilterAggregator.FilterOperation.LTE, 30d);
        runPercentRemainingTest(multiHistGroup, createDoubleGroup(0.5), filterAggregator, mergeAggregator);

    }

    private void runPercentRemainingTest(final List<DataPoint> startGroup, final DataPointGroup expected,
                                         final Aggregator... aggregators) {
        DataPointGroup aggregated = HistogramUtils.createGroup(startGroup);

        for (final Aggregator agg : aggregators) {
            aggregated = agg.aggregate(aggregated);
        }

        final DataPointGroup percentRemainingGroup = percentRemainingAggregator.aggregate(aggregated);

        while (expected.hasNext()) {
            assertTrue(percentRemainingGroup.hasNext());
            final DataPoint actual = percentRemainingGroup.next();
            assertEquals(expected.next(), actual);
        }
        assertFalse(percentRemainingGroup.hasNext());
    }

    private static DataPointGroup createDoubleGroup(final double... values) {
        final ListDataPointGroup group = new ListDataPointGroup("test_values");
        long ts = 1L;
        for (final double value : values) {
            group.addDataPoint(new DoubleDataPoint(ts++, value));
        }
        return group;
    }

    private void configureFilter(final FilterAggregator.FilterOperation op, final double threshold) {
        filterAggregator.setThreshold(threshold);
        filterAggregator.setFilterOp(op);
    }
}
