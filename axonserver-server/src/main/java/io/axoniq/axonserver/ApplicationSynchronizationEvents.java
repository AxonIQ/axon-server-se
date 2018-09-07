package io.axoniq.axonserver;

import io.axoniq.axonhub.internal.grpc.Applications;
import io.axoniq.platform.grpc.Application;

/**
 * Author: marc
 */
public class ApplicationSynchronizationEvents {

    @KeepNames
    public static class ApplicationsReceived {

        private final Applications applications;

        public ApplicationsReceived(Applications applications) {

            this.applications = applications;
        }

        public Applications getApplications() {
            return applications;
        }
    }

    @KeepNames
    public static class ApplicationReceived {
        private final Application application;
        private final boolean proxied;

        public ApplicationReceived(Application application, boolean proxied) {
            this.application = application;
            this.proxied = proxied;
        }

        public Application getApplication() {
            return application;
        }

        public boolean isProxied() {
            return proxied;
        }
    }

}
