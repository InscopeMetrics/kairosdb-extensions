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
package io.inscopemetrics.kairosdb.accumulators;

import com.arpnetworking.commons.math.Accumulator;
import com.google.common.base.MoreObjects;

/**
 * Factory which creates an {@link Accumulator} instance from a {@link Class}.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
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
