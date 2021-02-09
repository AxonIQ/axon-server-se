/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
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
 * @author Marc Gathier
 * @since 4.5
 */
public interface ConstraintCache<K, V> {

    int size();

    V remove(Object key);

    V get(Object key);

    V put(K key, V value);

    Collection<Map.Entry<K, V>> entrySet();
}
