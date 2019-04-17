/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.cli.json;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * @author Marc Gathier
 */
@JsonIgnoreProperties(ignoreUnknown=true)
public class ContextReplyNode {
    private String name;

    private String master;
    private String[] connectedNodes;

    public ContextReplyNode() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getMaster() {
        return master;
    }

    public void setMaster(String master) {
        this.master = master;
    }

    public String[] getConnectedNodes() {
        return connectedNodes;
    }

    public void setConnectedNodes(String[] connectedNodes) {
        this.connectedNodes = connectedNodes;
    }
}
