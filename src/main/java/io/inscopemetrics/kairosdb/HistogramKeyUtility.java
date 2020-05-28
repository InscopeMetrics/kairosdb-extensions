package io.inscopemetrics.kairosdb;

import com.google.common.collect.Maps;

import java.util.concurrent.ConcurrentMap;

/**
 * Utility functions for working with histogram keys of a given precision.
 *
 * @author Ville Koskela (ville dot koskela at inscopemertrics dot io)
 */
public final class HistogramKeyUtility {

    private static final int MANTISSA_BITS = 52;
    private static final int EXPONENT_BITS = 11;
    private static final long BASE_MASK = (1L << (MANTISSA_BITS + EXPONENT_BITS)) >> EXPONENT_BITS;
    private static final ConcurrentMap<Integer, HistogramKeyUtility> KEY_UTILITY_MAP = Maps.newConcurrentMap();

    private final int precision;
    private final long truncateMask;
    private final int packMask;
    private final int shift;

    private HistogramKeyUtility(final int precision) {
        this.precision = precision;
        truncateMask = BASE_MASK >> precision;
        packMask = (1 << (precision + EXPONENT_BITS + 1)) - 1;
        shift = MANTISSA_BITS - precision;
    }

    /**
     * Return the {@link HistogramKeyUtility} for the given precision.
     *
     * @param precision the target precision
     * @return the {@link HistogramKeyUtility} for the given precision
     */
    public static HistogramKeyUtility getInstance(final int precision) {
        return KEY_UTILITY_MAP.computeIfAbsent(precision, HistogramKeyUtility::new);
    }

    /**
     * Truncate a {@code double} key at given precision and represent it as a {@code long}.
     *
     * @param val the value to truncate
     * @return the truncated value as a {@code long}
     */
    public long truncateToLong(final double val) {
        return Double.doubleToRawLongBits(val) & truncateMask;
    }

    /**
     * Truncate a {@code double} key at a given precision and represent it as a {@code double}.
     *
     * @param val the value to truncate
     * @return the truncated value as a {@code double}
     */
    public double truncateToDouble(final double val) {
        return Double.longBitsToDouble(truncateToLong(val));
    }

    /**
     * Compute the largest magnitude (absolute value) of the bin the provided
     * value will be placed in.
     *
     * If the provided value is positive, the bound will be an inclusive upper
     * bound, and if the provided value is negative it will be an inclusive
     * lower bound.
     *
     * @param val the value to compute the bucket highest bucket
     * @return the inclusive upper or lower bound of the bin
     */
    public double binInclusiveBound(final double val) {
        long bound = Double.doubleToRawLongBits(val);
        bound >>= shift;
        bound++;
        bound <<= shift;
        bound--;
        return Double.longBitsToDouble(bound);
    }

    /**
     * Pack a {@code double} key for storage as a {@code long}. In addition to
     * truncation, this also shifts the value to optimize its size under varint
     * encoding.
     *
     * @param val the value to pack
     * @return the packed value as a {@code long}
     */
    long pack(final double val) {
        final long truncated = truncateToLong(val);
        final long shifted = truncated >> (MANTISSA_BITS - precision);
        return shifted & packMask;
    }

    /**
     * Unpack a {@code double} key from storage as a {@code long}.
     *
     * @param packed the packed value
     * @return the unpacked value as a {@code double}
     */
    double unpack(final long packed) {
        return Double.longBitsToDouble(packed << (MANTISSA_BITS - precision));
    }
}
