package io.axoniq.axonserver.applicationevents;


import io.axoniq.axonserver.access.jpa.User;

/**
 * Author: marc
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
