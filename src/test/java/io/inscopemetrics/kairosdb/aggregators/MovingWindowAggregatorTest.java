/*
 * Copyright 2018 Dropbox Inc.
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

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.Sampling;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.TimeUnit;
import org.kairosdb.testing.ListDataPointGroup;

import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Test class for {@link MovingWindowAggregator}.
 *
 * @author Gil Markham (gmarkham at dropbox dot com)
 */
public final class MovingWindowAggregatorTest {
    @Test
    public void testMultipleMonthAggregationWithUTC() {
        final ListDataPointGroup dpGroup = new ListDataPointGroup("range_group");
        final DateTimeZone utc = DateTimeZone.UTC;

        final DateTime startDate = new DateTime(2014, 1, 1, 1, 1, utc); // LEAP year
        final DateTime stopDate = new DateTime(2014, 7, 1, 1, 1, utc);
        for (DateTime iterationDT = startDate; iterationDT.isBefore(stopDate); iterationDT = iterationDT.plusDays(1)) {
            dpGroup.addDataPoint(new LongDataPoint(iterationDT.getMillis(), 1));
        }

        final MovingWindowAggregator agg = new MovingWindowAggregator();
        agg.setSampling(new Sampling(3, TimeUnit.MONTHS));
        agg.setAlignSampling(true);
        agg.setStartTime(startDate.getMillis());

        final DataPointGroup dpg = agg.aggregate(dpGroup);

        final SortedMap<Long, Long> dpCountMap = new TreeMap<>();

        while (dpg.hasNext()) {
            final DataPoint next = dpg.next();
            dpCountMap.compute(next.getTimestamp(), (key, value) -> {
                if (value == null) {
                    return 1L;
                } else {
                    return value + 1;
                }
            });
        }

        final Iterator<Map.Entry<Long, Long>> entryIter = dpCountMap.entrySet().iterator();
        assertThat(entryIter.hasNext(), is(true));
        Map.Entry<Long, Long> nextEntry = entryIter.next();
        assertThat(new DateTime(nextEntry.getKey(), utc), is(new DateTime(2014, 2, 1, 0, 0, utc)));
        assertThat(nextEntry.getValue(), is(31L)); // 31

        assertThat(entryIter.hasNext(), is(true));
        nextEntry = entryIter.next();
        assertThat(new DateTime(nextEntry.getKey(), utc), is(new DateTime(2014, 3, 1, 0, 0, utc)));
        assertThat(nextEntry.getValue(), is(59L)); // 31 + 28

        assertThat(entryIter.hasNext(), is(true));
        nextEntry = entryIter.next();
        assertThat(new DateTime(nextEntry.getKey(), utc), is(new DateTime(2014, 4, 1, 0, 0, utc)));
        assertThat(nextEntry.getValue(), is(90L)); // 31 + 28 + 31

        assertThat(entryIter.hasNext(), is(true));
        nextEntry = entryIter.next();
        assertThat(new DateTime(nextEntry.getKey(), utc), is(new DateTime(2014, 5, 1, 0, 0, utc)));
        assertThat(nextEntry.getValue(), is(89L)); // 28 + 31 + 30

        assertThat(entryIter.hasNext(), is(true));
        nextEntry = entryIter.next();
        assertThat(new DateTime(nextEntry.getKey(), utc), is(new DateTime(2014, 6, 1, 0, 0, utc)));
        assertThat(nextEntry.getValue(), is(92L)); // 31 + 30 + 31

        assertThat(entryIter.hasNext(), is(true));
        nextEntry = entryIter.next();
        assertThat(new DateTime(nextEntry.getKey(), utc), is(new DateTime(2014, 7, 1, 0, 0, utc)));
        assertThat(nextEntry.getValue(), is(91L)); // 30 + 31 + 30

        assertThat(entryIter.hasNext(), is(false));
    }

    @Test
    public void testMultipleRolling7DayAggregationWithUTC() {
        final ListDataPointGroup dpGroup = new ListDataPointGroup("range_group");
        final DateTimeZone utc = DateTimeZone.UTC;

        final DateTime startDate = new DateTime(2018, 1, 1, 1, 1, utc);
        final DateTime stopDate = new DateTime(2018, 1, 30, 1, 1, utc);
        for (DateTime iterationDT = startDate; iterationDT.isBefore(stopDate); iterationDT = iterationDT.plusDays(1)) {
            dpGroup.addDataPoint(new LongDataPoint(iterationDT.getMillis(), 1));
        }

        final MovingWindowAggregator agg = new MovingWindowAggregator();
        agg.setSampling(new Sampling(7, TimeUnit.DAYS));
        agg.setAlignSampling(true);
        agg.setStartTime(startDate.getMillis());

        final DataPointGroup dpg = agg.aggregate(dpGroup);

        final SortedMap<Long, Long> dpCountMap = new TreeMap<>();

        while (dpg.hasNext()) {
            final DataPoint next = dpg.next();
            dpCountMap.compute(next.getTimestamp(), (key, value) -> {
                if (value == null) {
                    return 1L;
                } else {
                    return value + 1;
                }
            });
        }

        final Iterator<Map.Entry<Long, Long>> entryIter = dpCountMap.entrySet().iterator();
        for (int i = 2; i <= 30; i++) {
            assertThat(entryIter.hasNext(), is(true));
            final Map.Entry<Long, Long> nextEntry = entryIter.next();
            assertThat(new DateTime(nextEntry.getKey(), utc), is(new DateTime(2018, 1, i, 0, 0, utc)));
            assertThat(nextEntry.getValue(), is(i < 8 ? i - 1 : 7L));
        }
    }

    @Test
    public void testMultipleUnalignedRolling7DayAggregationWithUTC() {
        final ListDataPointGroup dpGroup = new ListDataPointGroup("range_group");
        final DateTimeZone utc = DateTimeZone.UTC;

        final DateTime startDate = new DateTime(2018, 1, 1, 1, 1, utc);
        final DateTime stopDate = new DateTime(2018, 1, 30, 1, 1, utc);
        for (DateTime iterationDT = startDate; iterationDT.isBefore(stopDate); iterationDT = iterationDT.plusDays(1)) {
            dpGroup.addDataPoint(new LongDataPoint(iterationDT.getMillis(), 1));
        }

        final MovingWindowAggregator agg = new MovingWindowAggregator();
        agg.setSampling(new Sampling(7, TimeUnit.DAYS));
        agg.setAlignSampling(false);
        agg.setStartTime(startDate.getMillis());

        final DataPointGroup dpg = agg.aggregate(dpGroup);

        final SortedMap<Long, Long> dpCountMap = new TreeMap<>();

        while (dpg.hasNext()) {
            final DataPoint next = dpg.next();
            dpCountMap.compute(next.getTimestamp(), (key, value) -> {
                if (value == null) {
                    return 1L;
                } else {
                    return value + 1;
                }
            });
        }

        final Iterator<Map.Entry<Long, Long>> entryIter = dpCountMap.entrySet().iterator();
        for (int i = 1; i <= 29; i++) {
            assertThat(entryIter.hasNext(), is(true));
            final Map.Entry<Long, Long> nextEntry = entryIter.next();
            assertThat(new DateTime(nextEntry.getKey(), utc), is(new DateTime(2018, 1, i, 1, 1, utc)));
            assertThat(nextEntry.getValue(), is(i < 7 ? i : 7L));
        }
    }
}
