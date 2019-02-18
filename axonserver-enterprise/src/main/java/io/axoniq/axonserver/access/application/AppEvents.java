package io.axoniq.axonserver.access.application;

/**
 * Author: marc
 */
public class AppEvents {

    public static abstract class AppBaseEvent {
        private final String appName;

        protected AppBaseEvent(String appName) {
            this.appName = appName;
        }

        public String appName() {
            return appName;
        }
    }

    public static class AppUpdated extends AppBaseEvent {

        public AppUpdated(String appName) {
            super(appName);
        }
    }

    public static class AppDeleted extends AppBaseEvent {

        public AppDeleted(String appName) {
            super(appName);
        }
    }
}
