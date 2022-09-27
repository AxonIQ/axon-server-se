/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */
package io.axoniq.axonserver.access;

/**
 * {@link java.security.Principal} when the request is authenticated using an application token.
 */
public class ApplicationBinding {

    private final String name;

    public ApplicationBinding(String name) {
        this.name = name;
    }

    public String name() {
        return name;
    }

    @Override
    public String toString() {
        return "Application: " + name;
    }
}
