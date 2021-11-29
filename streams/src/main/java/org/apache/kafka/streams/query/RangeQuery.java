/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.query;

import java.util.Optional;

public class RangeQuery<K, V> implements Query<V> {

    private final Optional<K> lower;
    private final Optional<K> upper;

    private RangeQuery(final Optional<K> lower, final Optional<K> upper) {
        this.lower = lower;
        this.upper = upper;
    }

    public static <K, V> RangeQuery<K, V> withRange(final K lower, final K upper) {
        return new RangeQuery<>(Optional.of(lower), Optional.of(upper));
    }

    public static <K, V> RangeQuery<K, V> withUpperBound(final K upper) {
        return new RangeQuery<>(Optional.empty(), Optional.of(upper));
    }

    public static <K, V> RangeQuery<K, V> withLowerBound(final K lower) {
        return new RangeQuery<>(Optional.of(lower), Optional.empty());
    }

    public static <K, V> RangeQuery<K, V> withNoBounds() {
        return new RangeQuery<>(Optional.empty(), Optional.empty());
    }

    public Optional<K> getLowerBound() {
        return lower;
    }

    public Optional<K> getUpperBound() {
        return upper;
    }
}