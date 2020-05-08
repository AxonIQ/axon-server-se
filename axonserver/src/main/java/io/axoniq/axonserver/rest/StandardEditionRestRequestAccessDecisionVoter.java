/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.AccessDecisionVoter;
import org.springframework.security.access.ConfigAttribute;
import org.springframework.security.core.Authentication;
import org.springframework.security.web.FilterInvocation;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

/**
 * @author Marc Gathier
 */
public class StandardEditionRestRequestAccessDecisionVoter implements AccessDecisionVoter<FilterInvocation> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RestRequestAccessDecisionVoter.class);

    @Override
    public boolean supports(ConfigAttribute configAttribute) {
        return true;
    }

    @Override
    public boolean supports(Class aClass) {
        return FilterInvocation.class.isAssignableFrom(aClass);
    }

    @Override
    public int vote(Authentication authentication, FilterInvocation invocation,
                    Collection<ConfigAttribute> configAttributes) {
        if (permitAll(configAttributes)) {
            LOGGER.debug("PermitAll {}", invocation.getHttpRequest().getRequestURI());
            return AccessDecisionVoter.ACCESS_GRANTED;
        }
        if (invocation.getHttpRequest().getUserPrincipal() == null) {
            return AccessDecisionVoter.ACCESS_DENIED;
        }
        Set<String> rolesWithAccess = rolesForOperation(invocation.getHttpRequest().getMethod(),
                                                        invocation.getHttpRequest().getRequestURI());
        if (!rolesWithAccess.isEmpty() && !authorized(authentication, rolesWithAccess)) {
            return AccessDecisionVoter.ACCESS_DENIED;
        }
        return AccessDecisionVoter.ACCESS_GRANTED;
    }

    private Set<String> rolesForOperation(String method, String url) {
        if (url.startsWith("/v1/users")) {
            return Collections.singleton("ADMIN");
        }
        return Collections.emptySet();
    }

    private boolean permitAll(Collection<ConfigAttribute> configAttributes) {
        return configAttributes.stream()
                               .anyMatch(c -> "permitAll".equals(c.toString()));
    }


    private boolean authorized(Authentication authentication, Set<String> rolesWithAccess) {
        return authentication.getAuthorities().stream()
                             .anyMatch(a -> rolesWithAccess.contains(a.getAuthority()));
    }
}
