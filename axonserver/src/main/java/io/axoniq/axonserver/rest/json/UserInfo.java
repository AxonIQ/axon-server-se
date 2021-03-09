/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest.json;

import java.util.Set;

/**
 * @author Marc Gathier
 */
public class UserInfo {
    public static final String ROLE_ADMIN = "ADMIN";
    public static final String ROLE_ADMIN_IN_ADMIN = "ADMIN@_admin";
    public static final String ROLE_READONLY_ADMIN_IN_ADMIN = "VIEW_CONFIGURATION@_admin";

    private final String user;
    private final Set<String> roles;
    private final boolean adminUser;
    private final boolean readOnlyAdminUser;


    public UserInfo(String user, Set<String> roles) {
        this.user = user;
        this.roles = roles;
        this.adminUser = roles.contains(ROLE_ADMIN_IN_ADMIN) || roles.contains(ROLE_ADMIN);
        this.readOnlyAdminUser = roles.contains(ROLE_READONLY_ADMIN_IN_ADMIN);
    }

    public String getUser() {
        return user;
    }

    public Set<String> getRoles() {
        return roles;
    }

    public boolean isAdminUser() {
        return adminUser;
    }

    public boolean isReadOnlyAdminUser() {
        return readOnlyAdminUser;
    }
}
