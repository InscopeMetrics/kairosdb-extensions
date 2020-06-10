/*
 * Copyright 2020 Dropbox
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

package io.inscopemetrics.kairosdb.accumulators;

import com.arpnetworking.commons.math.Accumulator;
import org.junit.Test;

/**
 * Tests for {@link AccumulatorFromClassFactory}.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class AccumulatorFromClassFactoryTest {

    @Test(expected = RuntimeException.class)
    public void testCreateThrows() {
        new AccumulatorFromClassFactory(ThrowingAccumulator.class).create();
    }

    /**
     * Accumulator which cannot be instantiated.
     */
    public static final class ThrowingAccumulator implements Accumulator {

        private ThrowingAccumulator() {
            // Scope prevents instantiation
        }

        @Override
        public void accumulate(final double value) {
            // Intentionally empty
        }

        @Override
        public double getSum() {
            return 0d;
        }
    }
}
