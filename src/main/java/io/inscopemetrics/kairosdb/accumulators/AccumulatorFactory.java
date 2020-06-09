package io.inscopemetrics.kairosdb.accumulators;

import com.arpnetworking.commons.math.Accumulator;

/**
 * Factory interface for creating {@link Accumulator} instances.
 *
 * @author Ville Koskela (ville at koskilabs dot com)
 */
public interface AccumulatorFactory {

    /**
     * Create a new {@link Accumulator} instance.
     *
     * @return a new {@link Accumulator} instance
     */
    Accumulator create();
}
