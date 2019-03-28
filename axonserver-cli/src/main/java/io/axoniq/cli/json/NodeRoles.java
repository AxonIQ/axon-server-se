/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.cli.json;

/**
 * @author Marc Gathier
 */
public class NodeRoles {
    private String name;
    private boolean storage;
    private boolean messaging;

    public NodeRoles() {
    }

    public NodeRoles(String name, boolean messaging, boolean storage) {
        this.name = name;
        this.messaging = messaging;
        this.storage = storage;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isStorage() {
        return storage;
    }

    public void setStorage(boolean storage) {
        this.storage = storage;
    }

    public boolean isMessaging() {
        return messaging;
    }

    public void setMessaging(boolean messaging) {
        this.messaging = messaging;
    }

    @Override
    public String toString() {
        return name;
    }
}
