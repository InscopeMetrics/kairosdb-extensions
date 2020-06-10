/*
 * Copyright 2017 SmartSheet.com
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
import com.arpnetworking.commons.math.NaiveAccumulator;
import com.google.common.collect.Lists;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.inscopemetrics.kairosdb.accumulators.AccumulatorFactory;
import io.inscopemetrics.kairosdb.accumulators.AccumulatorFromClassFactory;
import io.inscopemetrics.kairosdb.aggregators.DelegatingAvgAggregator;
import io.inscopemetrics.kairosdb.aggregators.DelegatingCountAggregator;
import io.inscopemetrics.kairosdb.aggregators.DelegatingFilterAggregator;
import io.inscopemetrics.kairosdb.aggregators.DelegatingMaxAggregator;
import io.inscopemetrics.kairosdb.aggregators.DelegatingMinAggregator;
import io.inscopemetrics.kairosdb.aggregators.DelegatingPercentileAggregator;
import io.inscopemetrics.kairosdb.aggregators.DelegatingStdDevAggregator;
import io.inscopemetrics.kairosdb.aggregators.DelegatingSumAggregator;
import io.inscopemetrics.kairosdb.aggregators.HistogramApdexAggregator;
import io.inscopemetrics.kairosdb.aggregators.HistogramCountAggregator;
import io.inscopemetrics.kairosdb.aggregators.HistogramFilterAggregator;
import io.inscopemetrics.kairosdb.aggregators.HistogramMaxAggregator;
import io.inscopemetrics.kairosdb.aggregators.HistogramMeanAggregator;
import io.inscopemetrics.kairosdb.aggregators.HistogramMergeAggregator;
import io.inscopemetrics.kairosdb.aggregators.HistogramMinAggregator;
import io.inscopemetrics.kairosdb.aggregators.HistogramPercentRemainingAggregator;
import io.inscopemetrics.kairosdb.aggregators.HistogramPercentileAggregator;
import io.inscopemetrics.kairosdb.aggregators.HistogramStdDevAggregator;
import io.inscopemetrics.kairosdb.aggregators.HistogramSumAggregator;
import io.inscopemetrics.kairosdb.aggregators.MovingWindowAggregator;
import io.inscopemetrics.kairosdb.processors.MovingWindowQueryPreProcessor;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.core.aggregator.AvgAggregator;
import org.kairosdb.core.aggregator.CountAggregator;
import org.kairosdb.core.aggregator.FilterAggregator;
import org.kairosdb.core.aggregator.MaxAggregator;
import org.kairosdb.core.aggregator.MinAggregator;
import org.kairosdb.core.aggregator.PercentileAggregator;
import org.kairosdb.core.aggregator.StdAggregator;
import org.kairosdb.core.aggregator.SumAggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.MissingResourceException;
import java.util.Properties;
import javax.inject.Provider;

/**
 * Guice injection module binds instances for plugin.
 *
 * TODO(ville): Rename this module.  This is a breaking change.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
@SuppressWarnings("unchecked")
public class HistogramModule extends AbstractModule {

    private static final String ACCUMULATOR_CLASS_PROPERTY = "kairosdb.inscopemetrics.extensions.accumulator.class";
    private static final String DEFAULT_ACCUMULATOR_CLASS_NAME = NaiveAccumulator.class.getName();
    private static final Logger LOGGER = LoggerFactory.getLogger(HistogramModule.class);

    @Override
    protected void configure() {
        LOGGER.info("Binding HistogramModule");
        bind(HistogramDataPointV2Factory.class).in(Scopes.SINGLETON);
        bind(HistogramDataPointFactory.class).in(Scopes.SINGLETON);

        bind(AccumulatorFactory.class).toProvider(AccumulatorFactoryProvider.class);

        bind(DelegatingAvgAggregator.class);
        bind(HistogramMeanAggregator.class);

        bind(DelegatingCountAggregator.class);
        bind(HistogramCountAggregator.class);

        bind(DelegatingMinAggregator.class);
        bind(HistogramMinAggregator.class);

        bind(DelegatingMaxAggregator.class);
        bind(HistogramMaxAggregator.class);

        bind(DelegatingSumAggregator.class);
        bind(HistogramSumAggregator.class);

        bind(DelegatingPercentileAggregator.class);
        bind(HistogramPercentileAggregator.class);

        bind(HistogramMergeAggregator.class);
        bind(HistogramApdexAggregator.class);

        bind(DelegatingStdDevAggregator.class);
        bind(HistogramStdDevAggregator.class);

        bind(MovingWindowAggregator.class);
        bind(MovingWindowQueryPreProcessor.class);

        bind(DelegatingFilterAggregator.class);
        bind(HistogramFilterAggregator.class);

        bind(HistogramPercentRemainingAggregator.class);
    }

    @Provides
    @Named("avg")
    @Singleton
    @SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD", justification = "called reflectively by guice")
    private DelegatingRangeAggregatorMap getAvgMap(
            final KairosDataPointFactory factory,
            final Provider<HistogramMeanAggregator> histProvider,
            final Provider<AvgAggregator> avgProvider) {
        return new DelegatingRangeAggregatorMap(factory, Lists.newArrayList(histProvider, avgProvider));
    }

    @Provides
    @Named("count")
    @Singleton
    @SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD", justification = "called reflectively by guice")
    private DelegatingRangeAggregatorMap getCountMap(
            final KairosDataPointFactory factory,
            final Provider<HistogramCountAggregator> histProvider,
            final Provider<CountAggregator> countProvider) {
        return new DelegatingRangeAggregatorMap(factory, Lists.newArrayList(histProvider, countProvider));
    }

    @Provides
    @Named("min")
    @Singleton
    @SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD", justification = "called reflectively by guice")
    private DelegatingRangeAggregatorMap getMinMap(
            final KairosDataPointFactory factory,
            final Provider<HistogramMinAggregator> histProvider,
            final Provider<MinAggregator> minProvider) {
        return new DelegatingRangeAggregatorMap(factory, Lists.newArrayList(histProvider, minProvider));
    }

    @Provides
    @Named("max")
    @Singleton
    @SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD", justification = "called reflectively by guice")
    private DelegatingRangeAggregatorMap getMaxMap(
            final KairosDataPointFactory factory,
            final Provider<HistogramMaxAggregator> histProvider,
            final Provider<MaxAggregator> maxProvider) {
        return new DelegatingRangeAggregatorMap(factory, Lists.newArrayList(histProvider, maxProvider));
    }

    @Provides
    @Named("sum")
    @Singleton
    @SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD", justification = "called reflectively by guice")
    private DelegatingRangeAggregatorMap getSumMap(
            final KairosDataPointFactory factory,
            final Provider<HistogramSumAggregator> histProvider,
            final Provider<SumAggregator> sumProvider) {
        return new DelegatingRangeAggregatorMap(factory, Lists.newArrayList(histProvider, sumProvider));
    }

    @Provides
    @Named("percentile")
    @Singleton
    @SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD", justification = "called reflectively by guice")
    private DelegatingRangeAggregatorMap getPercentileMap(
            final KairosDataPointFactory factory,
            final Provider<HistogramPercentileAggregator> histProvider,
            final Provider<PercentileAggregator> percentileProvider) {
        return new DelegatingRangeAggregatorMap(factory, Lists.newArrayList(histProvider, percentileProvider));
    }

    @Provides
    @Named("dev")
    @Singleton
    @SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD", justification = "called reflectively by guice")
    private DelegatingRangeAggregatorMap getStdDevMap(
            final KairosDataPointFactory factory,
            final Provider<HistogramStdDevAggregator> histProvider,
            final Provider<StdAggregator> stdDevProvider) {
        return new DelegatingRangeAggregatorMap(factory, Lists.newArrayList(histProvider, stdDevProvider));
    }

    @Provides
    @Named("filter")
    @Singleton
    @SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD", justification = "called reflectively by guice")
    private DelegatingAggregatorMap getFilterMap(
            final KairosDataPointFactory factory,
            final Provider<HistogramFilterAggregator> histProvider,
            final Provider<FilterAggregator> filterProvider) {
        return new DelegatingAggregatorMap(factory, Lists.newArrayList(histProvider, filterProvider));
    }

    static <T> Class<T> getClassForProperty(final Properties properties, final String propertyName) {
        final String className = properties.getProperty(propertyName, DEFAULT_ACCUMULATOR_CLASS_NAME);

        try {
            @SuppressWarnings("unchecked")
            final Class<T> clazz = (Class<T>) HistogramModule.class.getClassLoader().loadClass(className);
            return clazz;
        } catch (final ClassNotFoundException e) {
            throw new MissingResourceException("Unable to load class", className, propertyName);
        }
    }

    static final class AccumulatorFactoryProvider implements Provider<AccumulatorFactory> {

        private final Properties properties;

        @Inject
        AccumulatorFactoryProvider(final Properties properties) {
            this.properties = properties;
        }

        @Override
        public AccumulatorFactory get() {
            final Class<? extends Accumulator> accumulatorClass = getClassForProperty(properties, ACCUMULATOR_CLASS_PROPERTY);
            return new AccumulatorFromClassFactory(accumulatorClass);
        }
    }
}
