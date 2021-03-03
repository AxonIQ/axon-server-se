/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.AxonServerAccessController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.AccessDecisionVoter;
import org.springframework.security.access.ConfigAttribute;
import org.springframework.security.core.Authentication;
import org.springframework.security.web.FilterInvocation;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;

/**
 * AccessDecisionVoter that determines if a specific request is allowed for the caller. Checks based on the roles
 * containing the operation and on the roles granted to the caller (app or user).
 *
 * @author Marc Gathier
 * @since 4.2
 */
public class RestRequestAccessDecisionVoter implements AccessDecisionVoter<FilterInvocation> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RestRequestAccessDecisionVoter.class);

    private final AxonServerAccessController axonServerAccessController;

    public RestRequestAccessDecisionVoter(AxonServerAccessController axonServerAccessController) {
        this.axonServerAccessController = axonServerAccessController;
    }

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
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("PermitAll {}", invocation.getHttpRequest().getRequestURI());
            }
            return AccessDecisionVoter.ACCESS_GRANTED;
        }
        String operation = invocation.getHttpRequest().getMethod() + ":" + invocation.getHttpRequest().getRequestURI();
        if (invocation.getHttpRequest().getUserPrincipal() == null) {
            LOGGER.debug("Vote result: DENIED for '{}', no principal in request.", operation);
            return AccessDecisionVoter.ACCESS_DENIED;
        }
        String context = context(invocation.getHttpRequest());
        Set<String> rolesWithAccess = axonServerAccessController.rolesForOperation(operation);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Vote for: '{}', auth = '{}', context = '{}', authorities: {}, authoritiesWithAccess: {} ", operation,
                    authentication.getName(), context, authentication.getAuthorities(), rolesWithAccess);
        }
        if (!rolesWithAccess.isEmpty() && !authorizedForContext(authentication, context, rolesWithAccess)) {
            LOGGER.debug("Vote result: DENIED for '{}'", operation);
            return AccessDecisionVoter.ACCESS_DENIED;
        }
        LOGGER.debug("Vote result: GRANTED for '{}'", operation);
        return AccessDecisionVoter.ACCESS_GRANTED;
    }

    private boolean permitAll(Collection<ConfigAttribute> configAttributes) {
        return configAttributes.stream()
                               .anyMatch(c -> "permitAll".equals(c.toString()));
    }

    private String context(HttpServletRequest o) {
        String context = o.getHeader(AxonServerAccessController.CONTEXT_PARAM);
        if (context == null) {
            context = o.getParameter(AxonServerAccessController.CONTEXT_PARAM);
        }
        if (context == null) {
            context = o.getParameter("context");
        }

        if (context == null) {
            context = axonServerAccessController.defaultContextForRest();
        }
        return context;
    }

    private boolean authorizedForContext(Authentication authentication, String context, Set<String> rolesWithAccess) {
        Set<String> contextRolesWithAccess = rolesWithAccess.stream()
                                                            .map(r -> r + '@' + context)
                                                            .collect(Collectors.toSet());

        return authentication.getAuthorities().stream()
                             .anyMatch(a -> contextRolesWithAccess.contains(a.getAuthority()));
    }
}
