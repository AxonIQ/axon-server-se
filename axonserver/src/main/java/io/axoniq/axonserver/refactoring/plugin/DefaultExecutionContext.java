/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.refactoring.plugin;

import io.axoniq.axonserver.plugin.ExecutionContext;
import io.axoniq.axonserver.refactoring.api.Authentication;
import io.axoniq.axonserver.refactoring.transport.rest.SpringAuthentication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Provides the context for intercepted requests. It contains information on the caller and the Axon Server context
 * of the request. The same context object is used for the whole interceptor chain for a request.
 *
 * @author Marc Gathier
 * @since 4.5
 */
public class DefaultExecutionContext implements ExecutionContext {

    private static final Logger logger = LoggerFactory.getLogger(DefaultExecutionContext.class);

    private final String context;
    private final Authentication principal;
    private final List<BiConsumer<Throwable, ExecutionContext>> compensatingActions = new LinkedList<>();
    private final Map<String, Object> details = new HashMap<>();


    /**
     * @param context   the Axon Server context for the request
     * @param principal the caller's information
     */
    @Deprecated
    public DefaultExecutionContext(String context, org.springframework.security.core.Authentication principal) {
        this(context, new SpringAuthentication(principal));
    }

    /**
     * @param context   the Axon Server context for the request
     * @param principal the caller's information
     */
    public DefaultExecutionContext(String context, Authentication principal) {
        this.context = context;
        this.principal = principal;
    }

    /**
     * Returns the Axon Server context for the request.
     *
     * @return the Axon Server context
     */
    @Override
    public String contextName() {
        return context;
    }

    /**
     * Returns the caller's information.
     *
     * @return the caller's information
     */
    @Override
    public String principal() {
        if (principal == null) {
            return "";
        }
        return principal.name();
    }

    @Override
    public Set<String> principalRoles() {
        if (principal == null) {
            return Collections.emptySet();
        }
        return principal.roles();
    }

    @Override
    public Map<String, String> principalTags() {
        return principal.detailsKeys()
                        .stream()
                        .collect(Collectors.toMap(key -> key, principal::detail));
    }

    /**
     * Registers an action to call when request execution fails, or any of the subsequent interceptors fail.
     *
     * @param compensatingAction runnable compensation action
     */
    @Override
    public void onFailure(BiConsumer<Throwable, ExecutionContext> compensatingAction) {
        compensatingActions.add(0, compensatingAction);
    }

    /**
     * Execute all compensating actions. The last registered compensating action is executed first.
     */
    public void compensate(Throwable cause) {
        compensatingActions.forEach(a -> {
            try {
                a.accept(cause, this);
            } catch (Exception ex) {
                logger.warn("{}: Failure during compensate due to {}", context, cause.getMessage(), ex);
            }
        });
    }

    /**
     * Add data to the interceptor context to be used at a later point in the interceptor chain.
     *
     * @param key   a key for the data
     * @param value the value
     */
    @Override
    public void putAttribute(String key, Object value) {
        details.put(key, value);
    }

    /**
     * Retrieves custom data from the interceptor context.
     *
     * @param key the key of the data
     * @return the value
     */
    @Override
    public Object getAttribute(String key) {
        return details.get(key);
    }
}
