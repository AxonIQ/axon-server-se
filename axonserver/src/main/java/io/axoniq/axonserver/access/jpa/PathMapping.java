/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.access.jpa;

import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * Maps a path to its required role.
 * The path may be a http method followed by a uri or a gRPC service followed by the operation name.
 * @author Marc Gathier
 */
@Entity
public class PathMapping {
    @Id
    private String path;

    private String role;

    public PathMapping() {
    }

    public PathMapping(String path, String role) {
        this.path = path;
        this.role = role;
    }

    public String getPath() {
        return path;
    }

    public String getRole() {
        return role;
    }
}
