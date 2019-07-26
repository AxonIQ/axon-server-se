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
import io.axoniq.axonserver.access.jpa.PathMapping;
import io.axoniq.axonserver.access.pathmapping.PathMappingRepository;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

/**
 * Created by marc on 7/17/2017.
 */
@Component
public class AxonServerStandardAccessController implements AxonServerAccessController {

    private PathMappingRepository pathMappingRepository;
    private final MessagingPlatformConfiguration messagingPlatformConfiguration;

    public AxonServerStandardAccessController(PathMappingRepository pathMappingRepository, MessagingPlatformConfiguration messagingPlatformConfiguration) {
        this.pathMappingRepository = pathMappingRepository;
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
    }

    @Override
    public boolean allowed(String fullMethodName, String context, String token) {
        return isTokenFromConfigFile(token);
    }

    @Override
    public Collection<PathMapping> getPathMappings() {
        return pathMappingRepository.findAll();
    }

    @Override
    public boolean isRoleBasedAuthentication() {
        return false;
    }

    @Override
    public Set<String> getRoles(String token) {
        return isTokenFromConfigFile(token) ? Sets.newHashSet("ADMIN@_admin") : Collections.emptySet();
    }

    private boolean isTokenFromConfigFile(String token) {
        return messagingPlatformConfiguration.getAccesscontrol().getToken().equals(token);
    }
}
