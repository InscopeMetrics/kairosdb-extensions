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

import com.arpnetworking.commons.math.BigDecimalAccumulator;
import com.arpnetworking.commons.math.KahanAccumulator;
import com.arpnetworking.commons.math.NaiveAccumulator;
import com.arpnetworking.commons.math.NeumaierAccumulator;
import com.arpnetworking.commons.math.PairwiseAccumulator;
import com.google.common.collect.Lists;
import io.inscopemetrics.kairosdb.accumulators.AccumulatorFromClassFactory;
import org.kairosdb.core.DataPoint;
import org.kairosdb.testing.HistogramUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Aggregator test helper.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public final class AggregatorTestHelper {

    private static final CreateHistogramFromValues CREATE_HISTOGRAM_V2_FROM_VALUES =
            HistogramUtils::createHistogramV2;
    private static final CreateHistogramFromValues CREATE_HISTOGRAM_V1_FROM_VALUES =
            HistogramUtils::createHistogramV1;
    private static final CreateHistogramFromCounts CREATE_HISTOGRAM_V2_FROM_COUNTS =
            HistogramUtils::createHistogramV2;
    private static final CreateHistogramFromCounts CREATE_HISTOGRAM_V1_FROM_COUNTS =
            HistogramUtils::createHistogramV1;

    private static final List<Object[]> ALL_PARAMETERS_BY_TYPE = Arrays.asList(
            new Object[]{
                    CREATE_HISTOGRAM_V1_FROM_VALUES,
                    CREATE_HISTOGRAM_V1_FROM_COUNTS
            },
            new Object[]{
                    CREATE_HISTOGRAM_V2_FROM_VALUES,
                    CREATE_HISTOGRAM_V2_FROM_COUNTS
            });

    private static final List<Object[]> FROM_VALUES_PARAMETERS_BY_TYPE = Arrays.asList(
            new Object[]{
                    CREATE_HISTOGRAM_V1_FROM_VALUES
            },
            new Object[]{
                    CREATE_HISTOGRAM_V2_FROM_VALUES
            });

    private static final List<Object[]> FROM_COUNTS_PARAMETERS_BY_TYPE = Arrays.asList(
            new Object[]{
                    CREATE_HISTOGRAM_V1_FROM_COUNTS
            },
            new Object[]{
                    CREATE_HISTOGRAM_V2_FROM_COUNTS
            });

    private AggregatorTestHelper() { }

    /**
     * Create a parameterization per histogram type providing both from values
     * and from counts factories.
     *
     * @return The resulting parameterization by factory.
     */
    public static Collection<Object[]> createParameters() {
        return createParameterization(ALL_PARAMETERS_BY_TYPE, Collections.emptyList());
    }

    /**
     * Create a parameterization per histogram type providing both from values
     * and from counts factories.
     *
     * @param parameterization Caller specified parameterization to be included.
     * @return The resulting parameterization by factory.
     */
    public static Collection<Object[]> createParameters(final Collection<Object[]> parameterization) {
        return createParameterization(ALL_PARAMETERS_BY_TYPE, parameterization);
    }

    /**
     * Create a parameterization per histogram type providing only from values
     * factories.
     *
     * @return The resulting parameterization by factory.
     */
    public static Collection<Object[]> createParametersFromValues() {
        return createParameterization(FROM_VALUES_PARAMETERS_BY_TYPE, Collections.emptyList());
    }

    /**
     * Create a parameterization per histogram type providing only from values
     * factories.
     *
     * @param parameterization Caller specified parameterization to be included.
     * @return The resulting parameterization by factory.
     */
    public static Collection<Object[]> createParametersFromValues(final Collection<Object[]> parameterization) {
        return createParameterization(FROM_VALUES_PARAMETERS_BY_TYPE, parameterization);
    }

    /**
     * Create a parameterization per histogram type providing only from counts
     * factories.
     *
     * @return The resulting parameterization by factory.
     */
    public static Collection<Object[]> createParametersFromCounts() {
        return createParameterization(FROM_COUNTS_PARAMETERS_BY_TYPE, Collections.emptyList());
    }

    /**
     * Create a parameterization per histogram type providing only from counts
     * factories.
     *
     * @param parameterization Caller specified parameterization to be included.
     * @return The resulting parameterization by factory.
     */
    public static Collection<Object[]> createParametersFromCounts(final Collection<Object[]> parameterization) {
        return createParameterization(FROM_COUNTS_PARAMETERS_BY_TYPE, parameterization);
    }

    /**
     * Create a parameterization of the {@link io.inscopemetrics.kairosdb.accumulators.AccumulatorFactory}
     * instances over the types of {@link com.arpnetworking.commons.math.Accumulator}.
     *
     * @return Parameterization of {@link io.inscopemetrics.kairosdb.accumulators.AccumulatorFactory} instances.
     */
    public static Collection<Object[]> createAccumulatorParameterizations() {
        return Lists.newArrayList(
                new Object[]{new AccumulatorFromClassFactory(NaiveAccumulator.class)},
                new Object[]{new AccumulatorFromClassFactory(PairwiseAccumulator.class)},
                new Object[]{new AccumulatorFromClassFactory(KahanAccumulator.class)},
                new Object[]{new AccumulatorFromClassFactory(NeumaierAccumulator.class)},
                new Object[]{new AccumulatorFromClassFactory(BigDecimalAccumulator.class)});
    }

    private static Collection<Object[]> createParameterization(
            final Collection<Object[]> factoryParameterizations,
            final Collection<Object[]> userParameterizations
    ) {
        final Collection<Object[]> parameterSets = new ArrayList<>();
        for (final Object[] factoryList : factoryParameterizations) {
            if (userParameterizations.isEmpty()) {
                parameterSets.add(factoryList);
            } else {
                for (final Object[] userParameterList : userParameterizations) {
                    final Object[] parameters = new Object[factoryList.length + userParameterList.length];
                    int i = 0;

                    // The first parameters are the factories
                    for (final Object factory : factoryList) {
                        parameters[i] = factory;
                        ++i;
                    }

                    // The second parameters are used supplied
                    for (final Object parameter : userParameterList) {
                        parameters[i] = parameter;
                        ++i;
                    }

                    // Add the parameter set
                    parameterSets.add(parameters);
                }
            }
        }
        return parameterSets;
    }

    /**
     * Interface for methods creating histograms from sample lists.
     */
    public interface CreateHistogramFromValues {

        /**
         * Create a histogram.
         *
         * @param timestamp the timestamp
         * @param values the measurements
         * @return the histogram
         */
        DataPoint create(long timestamp, Iterable<Double> values);
    }

    /**
     * Interface for methods creating histograms from sample to count maps.
     */
    public interface CreateHistogramFromCounts {

        /**
         * Create a histogram.
         *
         * @param timestamp the timestamp
         * @param counts the measurement counts
         * @return the histogram
         */
        DataPoint create(long timestamp, Map<Double, Long> counts);
    }
}
