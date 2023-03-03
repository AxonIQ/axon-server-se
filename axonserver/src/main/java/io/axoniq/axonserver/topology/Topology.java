/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.topology;

import io.axoniq.axonserver.api.Authentication;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Gives access to the nodes and contexts defined in the Axon Server configuration. For Standard Edition this only contains the current
 * node and the default context.
 *
 * @author Marc Gathier
 * @since 4.0
 */
public interface Topology {
    String DEFAULT_CONTEXT = "default";

    String getName();

    default boolean isMultiContext() {
        return false;
    }

    default boolean isActive(AxonServerNode node) {
        return true;
    }

    default boolean isLeader(String nodeName, String contextName) {
        return true;
    }

    default Stream<? extends AxonServerNode> nodes() {
        return Stream.of(getMe());
    }

    AxonServerNode getMe();

    default AxonServerNode findNodeForClient(String clientId,
                                             String componentName,
                                             String context,
                                             Map<String,String> tags) {
        return getMe();
    }

    /**
     * Gets the names of all contexts where the current Axon Server instance is member of. In Axon Server Standard this only contains DEFAULT_CONTEXT, in
     * Axon Server Enterprise this is dynamic.
     *
     * @return names of contexts
     */
    default Iterable<String> getMyContextNames() {
        return getMe().getContextNames();
    }

    /**
     * Checks if this node serves as administrative node for Axon Server. Always true for Standard Edition.
     *
     * @return true if this node is an administative node
     */
    default boolean isAdminNode() {
        return true;
    }

    /**
     * Gets the names of all contexts where the current Axon Server instance is member of, and it is storing events for.
     * In Axon Server Standard this only contains DEFAULT_CONTEXT, in Axon Server Enterprise this is dynamic.
     *
     * @return names of contexts
     */
    default Iterable<String> getMyStorageContextNames() {
        return getMyContextNames();
    }

    default boolean validContext(String context) {
        return true;
    }

    /**
     * Retrieves a {@link Set} of context names that the requester is allowed to see.
     * @param includeAdmin include the admin context in the result
     * @param authentication the authentication of the requester
     * @return set of context names
     */
    default Set<String> visibleContexts(boolean includeAdmin, Authentication authentication) {
        return Collections.singleton(DEFAULT_CONTEXT);
    }
}
