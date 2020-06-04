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

    private static final List<List<Object>> ALL_PARAMETERS_BY_TYPE = Arrays.asList(
            Arrays.asList(
                    CREATE_HISTOGRAM_V1_FROM_VALUES,
                    CREATE_HISTOGRAM_V1_FROM_COUNTS
            ),
            Arrays.asList(
                    CREATE_HISTOGRAM_V2_FROM_VALUES,
                    CREATE_HISTOGRAM_V2_FROM_COUNTS
            ));

    private static final List<List<Object>> FROM_VALUES_PARAMETERS_BY_TYPE = Arrays.asList(
            Arrays.asList(
                    CREATE_HISTOGRAM_V1_FROM_VALUES
            ),
            Arrays.asList(
                    CREATE_HISTOGRAM_V2_FROM_VALUES
            ));

    private static final List<List<Object>> FROM_COUNTS_PARAMETERS_BY_TYPE = Arrays.asList(
            Arrays.asList(
                    CREATE_HISTOGRAM_V1_FROM_COUNTS
            ),
            Arrays.asList(
                    CREATE_HISTOGRAM_V2_FROM_COUNTS
            ));

    /**
     * Create a parameterization per histogram type providing both from values
     * and from counts factories.
     *
     * @return The resulting parameterization by factory.
     */
    public static Collection<Object[]> createParameters() {
        // TODO(ville): Support the caller providing own parameterization.
        return createParameterization(ALL_PARAMETERS_BY_TYPE);
    }

    /**
     * Create a parameterization per histogram type providing only from values
     * factories.
     *
     * @return The resulting parameterization by factory.
     */
    public static Collection<Object[]> createParametersFromValues() {
        // TODO(ville): Support the caller providing own parameterization.
        return createParameterization(FROM_VALUES_PARAMETERS_BY_TYPE);
    }

    /**
     * Create a parameterization per histogram type providing only from counts
     * factories.
     *
     * @return The resulting parameterization by factory.
     */
    public static Collection<Object[]> createParametersFromCounts() {
        // TODO(ville): Support the caller providing own parameterization.
        return createParameterization(FROM_COUNTS_PARAMETERS_BY_TYPE);
    }

    private static Collection<Object[]> createParameterization(
            final List<List<Object>> parameterListA
    ) {
        // TODO(ville): Support the caller providing own parameterization.
        // NOTE: Then multiply the two paramaterizations together.
        final List<Object[]> parameterSets = new ArrayList<>();
        for (final List<Object> factoryList : parameterListA) {
            final Object[] parameters = new Object[factoryList.size()];

            // The first n parameters are the factories
            for (int i = 0; i < factoryList.size(); ++i) {
                parameters[i] = factoryList.get(i);
            }

            // Add the parameter set
            parameterSets.add(parameters);
        }
        return parameterSets;
    }

    protected interface CreateHistogramFromValues {

        DataPoint create(long timestamp, Iterable<Double> values);
    }

    protected interface CreateHistogramFromCounts {

        DataPoint create(long timestamp, Map<Double, Long> counts);
    }
}
