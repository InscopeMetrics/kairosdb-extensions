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
package io.inscopemetrics.kairosdb;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link HistogramKeyUtility}.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class HistogramKeyUtilityTest {

    @Test
    public void packAndUnpack() {
        final int precision = 7;
        final HistogramKeyUtility keyUtility = HistogramKeyUtility.getInstance(precision);

        // At precision 7 all values between -256 and 256 are fully represented
        for (double i = -256; i < 256; i += 1.0) {
            assertPacked(keyUtility, i);
        }

        // At precision 7 most of these values are truncated
        for (double i = -1000; i < 1000; i += 0.01) {
            assertPackedTruncated(keyUtility, i, 0.01);
        }
    }

    private void assertPacked(final HistogramKeyUtility keyUtility, final double val) {
        final long packed = keyUtility.pack(val);
        final double unpacked = keyUtility.unpack(packed);
        assertEquals(
                String.format("val: %f, unpacked: %f", val, unpacked),
                Double.doubleToLongBits(val),
                Double.doubleToLongBits(unpacked));
    }

    private void assertPackedTruncated(final HistogramKeyUtility keyUtility, final double val, final double error) {
        final long packed = keyUtility.pack(val);
        final double unpacked = keyUtility.unpack(packed);
        assertEquals(keyUtility.truncateToLong(val), Double.doubleToLongBits(unpacked));
        assertTrue(Math.abs(val - unpacked) / val < error);
    }
}
