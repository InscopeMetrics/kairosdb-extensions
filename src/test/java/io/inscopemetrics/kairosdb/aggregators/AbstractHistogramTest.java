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

import org.kairosdb.core.DataPoint;
import org.kairosdb.testing.HistogramUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Abstract class to help with parameterization of histogram aggregator tests.
 *
 * @author Ville Koskela (ville at koskilabs dot com)
 */
abstract class AbstractHistogramTest {

    private static final CreateHistogramFromValues CREATE_HISTOGRAM_V2_FROM_VALUES =
            HistogramUtils::createHistogramV2;
    private static final CreateHistogramFromValues CREATE_HISTOGRAM_V1_FROM_VALUES =
            HistogramUtils::createHistogramV1;
    private static final CreateHistogramFromCounts CREATE_HISTOGRAM_V2_FROM_COUNTS =
            HistogramUtils::createHistogramV2;
    private static final CreateHistogramFromCounts CREATE_HISTOGRAM_V1_FROM_COUNTS =
            HistogramUtils::createHistogramV1;

    /**
     * Create a parameterization per histogram from values factory of the
     * supplied parameters.
     *
     * @param parametersPerFactory The parameters to include with each factory.
     * @return The resulting parameterization by factory.
     */
    public static Collection<Object[]> createParametersForHistogramFromValues(
            final Collection<Object> parametersPerFactory) {
        final List<Object[]> parameterSets = new ArrayList<>();
        for (final CreateHistogramFromValues factory : getHistogramFactoriesFromValues()) {
            final Object[] parameters = new Object[parametersPerFactory.size() + 1];

            // The first parameter is the factory
            parameters[0] = factory;

            // The next parameters are the ones supplied
            int i = 1;
            for (final Object parameter : parametersPerFactory) {
                parameters[i] = parameter;
                ++i;
            }

            // Add the parameter set
            parameterSets.add(parameters);
        }
        return parameterSets;
    }

    /**
     * Create a parameterization per histogram from counts factory of the
     * supplied parameters.
     *
     * @param parametersPerFactory The parameters to include with each factory.
     * @return The resulting parameterization by factory.
     */
    public static Collection<Object[]> createParametersForHistogramFromCounts(
            final Collection<Object> parametersPerFactory) {
        final List<Object[]> parameterSets = new ArrayList<>();
        for (final CreateHistogramFromCounts factory : getHistogramFactoriesFromCounts()) {
            final Object[] parameters = new Object[parametersPerFactory.size() + 1];

            // The first parameter is the factory
            parameters[0] = factory;

            // The next parameters are the ones supplied
            int i = 1;
            for (final Object parameter : parametersPerFactory) {
                parameters[i] = parameter;
                ++i;
            }

            // Add the parameter set
            parameterSets.add(parameters);
        }
        return parameterSets;
    }

    /**
     * Return the available histogram from values factories.
     *
     * @return {@link Iterable} of histogram factories.
     */
    public static Iterable<CreateHistogramFromValues> getHistogramFactoriesFromValues() {
        return Arrays.asList(
                CREATE_HISTOGRAM_V1_FROM_VALUES,
                CREATE_HISTOGRAM_V2_FROM_VALUES);
    }

    /**
     * Return the available histogram from counts factories.
     *
     * @return {@link Iterable} of histogram factories.
     */
    public static Iterable<CreateHistogramFromCounts> getHistogramFactoriesFromCounts() {
        return Arrays.asList(
                CREATE_HISTOGRAM_V1_FROM_COUNTS,
                CREATE_HISTOGRAM_V2_FROM_COUNTS);
    }

    protected interface CreateHistogramFromValues {

        DataPoint create(long timestamp, Iterable<Double> values);
    }

    protected interface CreateHistogramFromCounts {

        DataPoint create(long timestamp, Map<Double, Long> counts);
    }
}
