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
import io.axoniq.axonserver.api.Authentication;

/**
 * @author Marc Gathier
 */
public abstract class UserEvents {
    public static class UserUpdated{

        private final User user;
        private final boolean proxied;
        private final Authentication implementer;

        public UserUpdated(User user, boolean proxied) {
            this(user, proxied, null);
        }

        public UserUpdated(User user, boolean proxied, Authentication implementer) {
            this.user = user;
            this.proxied = proxied;
            this.implementer = implementer;
        }

        public User getUser() {
            return user;
        }

        public boolean isProxied() {
            return proxied;
        }

        @Override
        public String toString() {
            return "UserUpdated{" +
                    "user=" + user +
                    ", proxied=" + proxied +
                    ", implementer=" + implementer +
                    '}';
        }
    }

    public static class UserDeleted  {

        private final String name;
        private final boolean proxied;
        private final Authentication implementer;

        public UserDeleted(String name, boolean proxied) {
            this(name, proxied, null);
        }

        public UserDeleted(String name, boolean proxied, Authentication implementer) {
            this.name = name;
            this.proxied = proxied;
            this.implementer = implementer;
        }

        public String getName() {
            return name;
        }

        public boolean isProxied() {
            return proxied;
        }

        @Override
        public String toString() {
            return "UserDeleted{" +
                    "name='" + name + '\'' +
                    ", proxied=" + proxied +
                    ", implementer=" + implementer +
                    '}';
        }
    }
}
