package io.axoniq.axonserver.enterprise;

import io.axoniq.platform.KeepNames;

/**
 * Author: marc
 */
public class ContextEvents {

    @KeepNames
    public static class ContextCreated {
        private final String context;

        public ContextCreated(String context) {
            this.context = context;
        }

        public String getContext() {
            return context;
        }
    }

}
