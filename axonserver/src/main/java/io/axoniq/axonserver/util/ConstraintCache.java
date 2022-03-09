/*
 * Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.util;

import java.util.Collection;
import java.util.Map;

/**
 * A map with limited operations in order to simplify the implementation of constraints need for caching items in
 * memory.
 *
 * @author Marc Gathier
 * @since 4.5
 */
public interface ConstraintCache<K, V> {

    /**
     * Returns the number of items in the cache.
     *
     * @return the number of items in the cache.
     */
    int size();

    /**
     * Removes the entry identified by the key from the cache.
     *
     * @param key the identifier of the item to be removed.
     * @return the item if was present, null otherwise
     */
    V remove(K key);

    /**
     * Retursn the entry identified by the key from the cache.
     *
     * @param key the identifier of the item to be returned.
     * @return the item if was present, null otherwise
     */
    V get(K key);

    /**
     * Puts inside the cache a new entry.
     *
     * @param key   the identifier of the item
     * @param value the item to be cached
     * @return the previus value if present, null otherwise
     */
    V put(K key, V value);

    /**
     * Returns the {@link Collection} of the entries in the cache
     *
     * @return the {@link Collection} of the entries in the cache
     */
    Collection<Map.Entry<K, V>> entrySet();
}
