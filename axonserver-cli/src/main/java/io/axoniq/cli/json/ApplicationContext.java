/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.cli.json;

import java.util.List;

/**
 * @author Marc Gathier
 */
public class ApplicationContext {
    private String context;
    private List<String> roles;

    public ApplicationContext() {
    }

    public ApplicationContext(String context, List<String> roleList) {
        this.context = context;
        this.roles = roleList;
    }

    public List<String> getRoles() {
        return roles;
    }

    public void setRoles(List<String> roles) {
        this.roles = roles;
    }

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    @Override
    public String toString() {
        return context + '{' + String.join(",", roles) + '}';
    }
}
