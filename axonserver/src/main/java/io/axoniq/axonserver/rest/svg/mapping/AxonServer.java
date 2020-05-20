/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest.svg.mapping;


import io.axoniq.axonserver.topology.AxonServerNode;

import java.util.Collections;
import java.util.Map;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public interface AxonServer {

    boolean isActive();

    AxonServerNode node();

    Iterable<String> contexts();

    Iterable<Storage> storage();

    default Map<String, String> tags() {
        return Collections.emptyMap();
    }

    default boolean isAdminLeader() {
        return false;
    }
}
