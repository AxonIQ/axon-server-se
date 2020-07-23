/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
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
public class ReplicationGroupJSON {

    /**
     * the name of the context
     */
    private String name;
    /**
     * the current leader of the context
     */
    private String leader;
    /**
     * List of current members and their roles
     */
    private List<NodeAndRole> roles = new ArrayList<>();
    /**
     * Indicator for pending changes on the context
     */
    private boolean changePending;
    /**
     * Timestamp of the start of the pending change
     */
    private long pendingSince;

    public ReplicationGroupJSON() {
    }

    public ReplicationGroupJSON(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getLeader() {
        return leader;
    }

    public void setLeader(String leader) {
        this.leader = leader;
    }

    public boolean isChangePending() {
        return changePending;
    }

    public void setChangePending(boolean changePending) {
        this.changePending = changePending;
    }

    public long getPendingSince() {
        return pendingSince;
    }

    public void setPendingSince(long pendingSince) {
        this.pendingSince = pendingSince;
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
