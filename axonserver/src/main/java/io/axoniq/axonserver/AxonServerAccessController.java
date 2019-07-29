/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver;


import com.google.common.collect.Sets;
import io.axoniq.axonserver.topology.Topology;

import java.util.Collections;
import java.util.Set;

/**
 * @author Marc Gathier
 */
public interface AxonServerAccessController {
    String TOKEN_PARAM = "AxonIQ-Access-Token";
    String AXONDB_TOKEN_PARAM = "Access-Token";
    String CONTEXT_PARAM = "AxonIQ-Context";

    boolean allowed(String fullMethodName, String context, String token);

    boolean isRoleBasedAuthentication();

    Set<String> getRoles(String token);

    default Set<String> rolesForOperation(String permission) {
        return Collections.emptySet();
    }

    default String defaultContextForRest() {
        return Topology.DEFAULT_CONTEXT;
    }

    default String usersByUsernameQuery() {
        return "select username,password, enabled from users where username=?";
    }

    default String authoritiesByUsernameQuery() {
        return "select username, role from user_roles where username=?";
    }

    default Set<String> rolesForLocalhost() {
        return Sets.newHashSet("ADMIN", "READ");
    }
}
