/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.access.jpa;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;

import java.util.Set;

/**
 * Entity for defined roles in Axon Server.
 *
 * @author Marc Gathier
 * @since 4.2
 */
@Entity
@Table(name = "ROLES")
public class Role {

    @Id
    private String role;

    private String description;

    @OneToMany(mappedBy = "role")
    private Set<FunctionRole> functions;

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Set<FunctionRole> getFunctions() {
        return functions;
    }

    public void setFunctions(Set<FunctionRole> functions) {
        this.functions = functions;
    }
}
