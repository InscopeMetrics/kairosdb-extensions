package io.inscopemetrics.kairosdb.accumulators;

import com.arpnetworking.commons.math.Accumulator;
import com.google.common.base.MoreObjects;

/**
 * Factory which creates an {@link Accumulator} instance from a {@link Class}.
 *
 * @author Ville Koskela (ville at koskilabs dot com)
 */
public final class AccumulatorFromClassFactory implements AccumulatorFactory {

    private final Class<? extends Accumulator> accumulatorClass;

    /**
     * Public constructor.
     *
     * @param accumulatorClass the implementation of {@link Accumulator} to create
     */
    public AccumulatorFromClassFactory(final Class<? extends Accumulator> accumulatorClass) {
        this.accumulatorClass = accumulatorClass;
    }

    @Override
    public Accumulator create() {
        try {
            return accumulatorClass.newInstance();
        } catch (final InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(AccumulatorFromClassFactory.class)
                .add("accumulatorClass", accumulatorClass.getSimpleName())
                .toString();
    }
}
