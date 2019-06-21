/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver;


import io.axoniq.axonserver.access.jpa.PathMapping;

import java.util.Collection;
import java.util.Set;

/**
 * @author Marc Gathier
 */
public interface AxonServerAccessController {
    String TOKEN_PARAM = "AxonIQ-Access-Token";
    String AXONDB_TOKEN_PARAM = "Access-Token";
    String CONTEXT_PARAM = "AxonIQ-Context";

    boolean allowed(String fullMethodName, String context, String token);

    Collection<PathMapping> getPathMappings();

    boolean isRoleBasedAuthentication();

    Set<String> getRoles(String token, String context);
}
