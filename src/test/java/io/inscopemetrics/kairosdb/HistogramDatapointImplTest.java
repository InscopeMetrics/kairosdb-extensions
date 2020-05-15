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
package io.inscopemetrics.kairosdb;

import org.junit.Test;

import java.util.TreeMap;

import static org.junit.Assert.assertEquals;

/**
 * Test class for {@link HistogramDataPointImpl}.
 *
 * @author William Ehlardt (whale at dropbox dot com)
 */
public class HistogramDatapointImplTest {
    @Test
    public void testLargeSampleCount() {
        final TreeMap<Double, Integer> map = new TreeMap<>();
        map.put(1d, 2147483647);
        map.put(2d, 2147483647);
        map.put(3d, 2147483647);
        map.put(4d, 2147483647);
        final HistogramDataPointImpl dp = new HistogramDataPointImpl(1, map, -10, 10, 10, 10);
        assertEquals(8589934588L, dp.getSampleCount());
    }
}
