package io.axoniq.axonserver.applicationevents;

import io.axoniq.platform.user.User;
import org.springframework.context.ApplicationEvent;

/**
 * Author: marc
 */
public abstract class UserEvents {
    public static class UserUpdated{

        private final User user;

        public UserUpdated(User user) {

            this.user = user;
        }

        public User getUser() {
            return user;
        }
    }

    public static class UserDeleted  {

        private final String name;

        public UserDeleted(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }
}
