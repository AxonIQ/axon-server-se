/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.applicationevents;


import io.axoniq.axonserver.access.jpa.User;

/**
 * @author Marc Gathier
 */
public abstract class UserEvents {
    public static class UserUpdated{

        private final User user;
        private final boolean proxied;

        public UserUpdated(User user, boolean proxied) {

            this.user = user;
            this.proxied = proxied;
        }

        public User getUser() {
            return user;
        }

        public boolean isProxied() {
            return proxied;
        }
    }

    public static class UserDeleted  {

        private final String name;
        private final boolean proxied;

        public UserDeleted(String name, boolean proxied) {
            this.name = name;
            this.proxied = proxied;
        }

        public String getName() {
            return name;
        }

        public boolean isProxied() {
            return proxied;
        }
    }
}
