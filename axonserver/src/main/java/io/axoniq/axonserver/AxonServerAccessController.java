/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver;


import io.axoniq.axonserver.topology.Topology;
import org.springframework.security.core.Authentication;

/**
 * Interface containing operations for access control.
 *
 * @author Marc Gathier
 * @since 4.0
 */
public interface AxonServerAccessController {
    String TOKEN_PARAM = "AxonIQ-Access-Token";
    String AXONDB_TOKEN_PARAM = "Access-Token";
    String CONTEXT_PARAM = "AxonIQ-Context";
    String PRINCIPAL_PARAM = "AxonIQ-Principal";

    /**
     * Checks if a specific method is allowed in given {@code context} for application {@code token}.
     *
     * @param fullMethodName the method name
     * @param context        the context
     * @param token          the app's token
     * @return true if method is allowed
     */
    boolean allowed(String fullMethodName, String context, String token);

    /**
     * Checks if a specific method is allowed in given {@code context} for {@code authentication}.
     *
     * @param fullMethodName the method name
     * @param context        the context
     * @param authentication the authentication information
     * @return true if method is allowed
     */
    boolean allowed(String fullMethodName, String context, Authentication authentication);

    /**
     * Returns the default context to authenticate against when the REST request does not specify a context.
     *
     * @return default context
     */
    default String defaultContextForRest() {
        return Topology.DEFAULT_CONTEXT;
    }

    /**
     * Returns (native SQL) query to retrieve username and password from the control database.
     * @return the query
     */
    default String usersByUsernameQuery() {
        return "select username,password, enabled from users where (username=?) and (password<>'nologon')";
    }

    /**
     * Returns (native SQL) query to retrieve the roles per user.
     *
     * @return the query
     */
    default String authoritiesByUsernameQuery() {
        return "select username, role from user_roles where username=?";
    }

    /**
     * @param context the name of the context
     * @param token   the token passed with the request
     * @return authentication information for the application
     *
     * @throws io.axoniq.axonserver.exception.InvalidTokenException when the token is unknown
     */
    Authentication authentication(String context, String token);
}
