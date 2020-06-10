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

import com.arpnetworking.commons.math.Accumulator;
import io.inscopemetrics.kairosdb.accumulators.AccumulatorFactory;
import org.junit.Test;

import java.util.MissingResourceException;
import java.util.Properties;
import javax.inject.Provider;

import static org.junit.Assert.assertEquals;

/**
 * Test class for {@link HistogramModule}.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public class HistogramModuleTest {

    @Test
    public void testAccumulatorFactoryProvider() {
        final Properties properties = new Properties();
        properties.setProperty(
                "kairosdb.inscopemetrics.extensions.accumulator.class",
                "io.inscopemetrics.kairosdb.HistogramModuleTest$TestAccumulator");
        final Provider<AccumulatorFactory> provider = new HistogramModule.AccumulatorFactoryProvider(properties);
        final AccumulatorFactory factory = provider.get();
        final Accumulator accumulator = factory.create();
        assertEquals(TestAccumulator.class, accumulator.getClass());
    }

    @Test
    public void testClassForProperty() {
        final Properties properties = new Properties();
        properties.setProperty("foo", "java.lang.String");
        final Class<?> clazz = HistogramModule.getClassForProperty(properties, "foo");
        assertEquals(String.class, clazz);
    }

    @Test(expected = MissingResourceException.class)
    public void testClassForPropertyDoesNotExist() {
        final Properties properties = new Properties();
        properties.setProperty("foo", "class.does.not.Exist");
        HistogramModule.getClassForProperty(properties, "foo");
    }

    /**
     * Test accumulator.
     */
    public static final class TestAccumulator implements Accumulator {

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
