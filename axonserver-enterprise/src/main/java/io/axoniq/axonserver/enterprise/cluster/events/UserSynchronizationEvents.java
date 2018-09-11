package io.axoniq.axonserver.enterprise.cluster.events;

import io.axoniq.axonhub.internal.grpc.Users;
import io.axoniq.axonserver.KeepNames;
import io.axoniq.platform.grpc.User;

/**
 * Author: marc
 */
public class UserSynchronizationEvents {

    @KeepNames
    public static class UsersReceived {
        private final Users users;

        public UsersReceived(Users users) {
            this.users = users;
        }

        public Users getUsers() {
            return users;
        }
    }

    @KeepNames
    public static class UserReceived {
        private final User user;
        private final boolean proxied;

        public UserReceived(User user, boolean proxied) {
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
}
