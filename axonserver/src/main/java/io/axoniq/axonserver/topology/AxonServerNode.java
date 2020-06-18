/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.topology;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Defines an AxonServer node.
 * @author Marc Gathier
 * @since 4.0
 */
public interface AxonServerNode {

    /**
     * The hostname that the node will communicate to clients.
     * @return the hostname
     */
    String getHostName();

    /**
     * The port number to be used by clients to connect to this node.
     * @return gRPC port number
     */
    Integer getGrpcPort();

    /**
     * The hostname used by other nodes in the cluster to connect to this node. This is only used in Enterprise Edition.
     * @return the internal hostname
     */
    String getInternalHostName();

    /**
     * The port number to be used by other nodes in the cluster to connect to this node. This is only used in Enterprise Edition.
     * @return gRPC port number
     */
    Integer getGrpcInternalPort();

    /**
     * The HTTP port number for rest services and dashboard.
     * @return HTTP port
     */
    Integer getHttpPort();

    /**
     * The name of the node.
     * @return the node name
     */
    String getName();

    /**
     * Returns a list of all contexts where this node is member of. In Standard Edition this will only contain "default".
     * @return
     */
    Collection<String> getContextNames();

    /**
     * Returns a list of all contexts where this node is member of and that can store events.
     * In Standard Edition this will only contain "default".
     *
     * @return
     */
    default Collection<String> getStorageContextNames() {
        return getContextNames();
    }

    default Map<String, String> getTags() {
        return Collections.emptyMap();
    }
}
