/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.access.jpa;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Id;

/**
 * Holds defined roles. Roles may be defined on an application or user level.
 *
 * @author Sara Pellegrini
 */
@Entity
public class Role {

    @Id
    private String name;

    @ElementCollection
    @Enumerated(EnumType.STRING)
    private Set<Type> types = new HashSet<>();


    public enum Type {
        USER,APPLICATION
    }

    public Role() {
    }

    public Role(String name, Type ... types) {
        this.name = name;
        this.types.addAll(Arrays.asList(types));
    }

    public String name() {
        return name;
    }
}
