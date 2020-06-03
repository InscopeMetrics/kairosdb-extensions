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

import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.inscopemetrics.kairosdb.HistogramDataPoint;
import io.inscopemetrics.kairosdb.HistogramDataPointV2Impl;
import io.inscopemetrics.kairosdb.HistogramKeyUtility;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.AggregatedDataPointGroupWrapper;
import org.kairosdb.core.aggregator.FilterAggregator;
import org.kairosdb.core.annotation.FeatureComponent;
import org.kairosdb.core.annotation.FeatureProperty;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.plugin.Aggregator;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Aggregator that filters away some bins of a histogram based on an operation and threshold.
 *
 * @author Joey Jackson (jjackson at dropbox dot com)
 */
@FeatureComponent(
        name = "hfilter",
        description = "Filters histograms according to filter operation")
public class HistogramFilterAggregator implements Aggregator {
    /**
     * Whether to keep or discard indeterminate buckets when filtering.
     */
    public enum FilterIndeterminate {
        /**
         * Keep the individual buckets in the data point group if it is indeterminate if they should be filtered by the
         * specific query.
         */
        KEEP {
            boolean shouldDiscard(final boolean thresholdAcceptsLowerBound, final boolean thresholdAcceptsUpperBound) {
                return thresholdAcceptsLowerBound && thresholdAcceptsUpperBound;
            }
        },
        /**
         * Discard the individual buckets in the data point group if it is indeterminate if they should be filtered by
         * the specific query.
         */
        DISCARD {
            boolean shouldDiscard(final boolean thresholdAcceptsLowerBound, final boolean thresholdAcceptsUpperBound) {
                return thresholdAcceptsLowerBound || thresholdAcceptsUpperBound;
            }
        };

        abstract boolean shouldDiscard(boolean thresholdAcceptsLowerBound, boolean thresholdAcceptsUpperBound);
    }

    @FeatureProperty(
            name = "filter_op",
            label = "Filter operation",
            description = "The operation performed for each data point.",
            type = "enum",
            options = {"lte", "lt", "gte", "gt", "equal"},
            default_value = "equal"
    )
    private FilterAggregator.FilterOperation filterOp;

    @FeatureProperty(
            name = "filter_indeterminate_inclusion",
            label = "Filter indeterminate inclusion",
            description = "Whether to keep or discard a histogram bin that straddles the threshold when filtering",
            type = "enum",
            options = {"keep", "discard"},
            default_value = "keep"
    )
    private FilterIndeterminate filterinc;

    @FeatureProperty(
            label = "Threshold",
            description = "The value the operation is performed on. If the operation is lt, then a null data point "
                    + "is returned if the data point is less than the threshold."
    )
    private double threshold;


    /**
     * Public Constructor.
     */
    @Inject
    public HistogramFilterAggregator() {
        threshold = 0.0;
        filterOp = FilterAggregator.FilterOperation.EQUAL;
        filterinc = FilterIndeterminate.KEEP;
    }

    public void setFilterOp(final FilterAggregator.FilterOperation filterOp) {
        this.filterOp = filterOp;
    }

    public void setThreshold(final double threshold) {
        this.threshold = threshold;
    }

    public void setFilterIndeterminateInclusion(final FilterIndeterminate inclusion) {
        filterinc = inclusion;
    }

    @Override
    public DataPointGroup aggregate(final DataPointGroup dataPointGroup) {
        return new HistogramFilterDataPointAggregator(dataPointGroup);
    }

    @Override
    public boolean canAggregate(final String groupType) {
        return HistogramDataPoint.GROUP_TYPE.equals(groupType);
    }

    @Override
    public String getAggregatedGroupType(final String groupType) {
        return HistogramDataPoint.GROUP_TYPE;
    }

    private static boolean isNegative(final double value) {
        // TODO(ville): Add documentation to why this is implemented this way.
        return Double.doubleToLongBits(value) < 0;
    }

    private class HistogramFilterDataPointAggregator extends AggregatedDataPointGroupWrapper {

        HistogramFilterDataPointAggregator(final DataPointGroup innerDataPointGroup) {
            super(innerDataPointGroup);
        }

        public boolean hasNext() {
            boolean foundValidDp = false;

            while (!foundValidDp && currentDataPoint != null) {
                final Optional<HistogramDataPoint> hdp = filterBins(currentDataPoint);
                if (hdp.isPresent()) {
                    currentDataPoint = hdp.get();
                    foundValidDp = true;
                } else {
                    moveCurrentDataPoint();
                }
            }

            return foundValidDp;
        }

        public DataPoint next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more data points exist");
            }
            final DataPoint ret = currentDataPoint;
            moveCurrentDataPoint();
            return ret;
        }

        private void moveCurrentDataPoint() {
            if (hasNextInternal()) {
                currentDataPoint = nextInternal();
            } else {
                currentDataPoint = null;
            }
        }

        private Optional<HistogramDataPoint> filterBins(final DataPoint dp) {
            if (!(dp instanceof HistogramDataPoint)) {
                return Optional.empty();
            }

            final long timeStamp = dp.getTimestamp();
            final TreeMap<Double, Long> filtered = Maps.newTreeMap();
            double min = Double.MAX_VALUE;
            double max = -Double.MAX_VALUE;
            double sum = 0;
            long count = 0;

            final HistogramDataPoint hist = (HistogramDataPoint) dp;
            final HistogramKeyUtility histogramKeyUtility = HistogramKeyUtility.getInstance(hist.getPrecision());
            final long originalCount = hist.getOriginalCount();

            if (histNotChangedByThreshold(hist)) {
                return Optional.of(hist);
            }

            for (final Map.Entry<Double, Long> entry : hist.getMap().entrySet()) {
                if (!shouldDiscard(entry.getKey(), histogramKeyUtility)) {
                    filtered.put(entry.getKey(), entry.getValue());
                    min = Math.min(
                            min,
                            Math.min(
                                    entry.getKey(),
                                    histogramKeyUtility.binInclusiveBound(entry.getKey())));
                    max = Math.max(
                            max,
                            Math.max(
                                    entry.getKey(),
                                    histogramKeyUtility.binInclusiveBound(entry.getKey())));
                    sum += entry.getKey() * entry.getValue();
                    count += entry.getValue();
                }
            }

            if (filtered.isEmpty()) {
                return Optional.empty();
            }

            // TODO(ville): If we know we're not going to change it then don't compute it above.
            if (minNotChangedByThreshold(hist.getMin(), histogramKeyUtility)) {
                min = hist.getMin();
            }
            if (maxNotChangedByThreshold(hist.getMax(), histogramKeyUtility)) {
                max = hist.getMax();
            }

            return Optional.of(
                    new HistogramDataPointV2Impl(
                            timeStamp,
                            hist.getPrecision(),
                            filtered,
                            min,
                            max,
                            sum / count,
                            sum,
                            originalCount));
        }

        private boolean histNotChangedByThreshold(final HistogramDataPoint hist) {
            switch (filterOp) {
                case GT:
                    return threshold >= hist.getMax();
                case GTE:
                    return threshold > hist.getMax();
                case LT:
                    return threshold <= hist.getMin();
                case LTE:
                    return threshold < hist.getMin();
                case EQUAL:
                    return threshold < hist.getMin() || hist.getMax() < threshold
                            && filterinc == FilterIndeterminate.DISCARD;
                default:
                    throw new IllegalStateException("Unsupported FilterOp Enum type");
            }
        }

        private boolean minNotChangedByThreshold(final double min, final HistogramKeyUtility histogramKeyUtility) {
            switch (filterOp) {
                case LT:
                    return threshold <= min;
                case LTE:
                    return threshold < min;
                case EQUAL:
                    return !shouldDiscard(min, histogramKeyUtility);
                default:
                    return true;
            }
        }

        private boolean maxNotChangedByThreshold(final double max, final HistogramKeyUtility histogramKeyUtility) {
            switch (filterOp) {
                case GT:
                    return threshold >= max;
                case GTE:
                    return threshold > max;
                case EQUAL:
                    return !shouldDiscard(max, histogramKeyUtility);
                default:
                    return true;
            }
        }

        private boolean shouldDiscard(final double value, final HistogramKeyUtility histogramKeyUtility) {
            final double lowerBound;
            final double upperBound;
            if (isNegative(value)) {
                upperBound = histogramKeyUtility.truncateToDouble(value);
                lowerBound = histogramKeyUtility.binInclusiveBound(value);
            } else {
                lowerBound = histogramKeyUtility.truncateToDouble(value);
                upperBound = histogramKeyUtility.binInclusiveBound(value);
            }

            //=================================================================
            /*
             * TODO(Joey Jackson):
             *  DESC. The refactored code below uses the .compare method added to the
             *  FilterOp Enum in the base KairosDB library. The pull request listed below
             *  must be merged before this cleaner version of the code can be used. Until then,
             *  the code below is incompatible with the base KairosDB library, but is compatible
             *  with the fork by ddimensia.
             *  Ref:
             *      https://github.com/kairosdb/kairosdb/pull/555
             *      https://github.com/ddimensia/kairosdb
            if (filterOp == FilterAggregator.FilterOperation.EQUAL) {
                if (filterinc == FilterIndeterminate.DISCARD) {
                    return threshold >= lowerBound && threshold <= upperBound;
                } else if (filterinc == FilterIndeterminate.KEEP) {
                    return false;
                }
            } else {
                final boolean thresholdAcceptsLowerBound = filterOp.compare(lowerBound, threshold);
                final boolean thresholdAcceptsUpperBound = filterOp.compare(upperBound, threshold);
                return filterinc.shouldDiscard(thresholdAcceptsLowerBound, thresholdAcceptsUpperBound);
            }
            */
            //=================================================================
            final boolean thresholdAcceptsLowerBound;
            final boolean thresholdAcceptsUpperBound;
            switch (filterOp) {
                case LTE:
                    thresholdAcceptsLowerBound = lowerBound <= threshold;
                    thresholdAcceptsUpperBound = upperBound <= threshold;
                    return filterinc.shouldDiscard(thresholdAcceptsLowerBound, thresholdAcceptsUpperBound);
                case LT:
                    thresholdAcceptsLowerBound = lowerBound < threshold;
                    thresholdAcceptsUpperBound = upperBound < threshold;
                    return filterinc.shouldDiscard(thresholdAcceptsLowerBound, thresholdAcceptsUpperBound);
                case GTE:
                    thresholdAcceptsLowerBound = lowerBound >= threshold;
                    thresholdAcceptsUpperBound = upperBound >= threshold;
                    return filterinc.shouldDiscard(thresholdAcceptsLowerBound, thresholdAcceptsUpperBound);
                case GT:
                    thresholdAcceptsLowerBound = lowerBound > threshold;
                    thresholdAcceptsUpperBound = upperBound > threshold;
                    return filterinc.shouldDiscard(thresholdAcceptsLowerBound, thresholdAcceptsUpperBound);
                case EQUAL:
                    if (filterinc == FilterIndeterminate.DISCARD) {
                        return threshold >= lowerBound && threshold <= upperBound;
                    } else if (filterinc == FilterIndeterminate.KEEP) {
                        return false;
                    } else {
                        throw new IllegalStateException("Unsupported FilterIndeterminateInclusion Enum type");
                    }
                default:
                    throw new IllegalStateException("Unsupported FilterOp Enum type");
            }
            //=================================================================
        }
    }
}
