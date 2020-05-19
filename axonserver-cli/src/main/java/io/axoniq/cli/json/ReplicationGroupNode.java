/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.cli.json;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Marc Gathier
 */
public class ReplicationGroupNode {

    private String context;
    private String leader;
    private List<NodeAndRole> roles = new ArrayList<>();


    public ReplicationGroupNode() {
    }

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }


    public String getLeader() {
        return leader;
    }

    public void setLeader(String leader) {
        this.leader = leader;
    }

    public List<NodeAndRole> getRoles() {
        return roles;
    }

    public void setRoles(List<NodeAndRole> roles) {
        this.roles = roles;
    }

    public boolean hasRoles() {
        return roles != null && !roles.isEmpty();
    }

    public String concatRoles() {
        return roles == null ? "" : roles.stream()
                                         .map(Object::toString)
                                         .collect(Collectors.joining(","));
    }
}
