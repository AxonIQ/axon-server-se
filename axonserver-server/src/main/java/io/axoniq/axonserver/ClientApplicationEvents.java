package io.axoniq.axonserver;

/**
 * Author: marc
 */
public class ClientApplicationEvents {

    public abstract static class ClientApplicationBaseEvent {
        private final boolean forwarded;

        protected ClientApplicationBaseEvent(boolean forwarded) {
            this.forwarded = forwarded;
        }

        public boolean isForwarded() {
            return forwarded;
        }
    }

    @KeepNames
    public static class ApplicationConnected extends ClientApplicationBaseEvent {
        private final String context;
        private final String componentName;
        private final String client;
        private final String proxy;

        public ApplicationConnected(String context, String componentName, String client, String proxy) {
            super(proxy != null);
            this.context = context;
            this.componentName = componentName;
            this.client = client;
            this.proxy = proxy;
        }
        public ApplicationConnected(String context, String componentName, String client) {
            this(context, componentName, client, null);
        }

        public String getComponentName() {
            return componentName;
        }

        public String getClient() {
            return client;
        }

        public String getContext() {
            return context;
        }

        public String getProxy() {
            return proxy;
        }

        public boolean isProxied() {
            return isForwarded();
        }
    }

    @KeepNames
    public static class ApplicationDisconnected extends ClientApplicationBaseEvent {
        private final String context;
        private final String componentName;
        private final String client;
        private final String proxy;

        public ApplicationDisconnected(String context, String componentName, String client, String proxy) {
            super(proxy != null);
            this.context = context;
            this.componentName = componentName;
            this.client = client;
            this.proxy = proxy;
        }

        public ApplicationDisconnected(String context,
                                       String componentName, String client
        ) {
            this(context, componentName, client, null);
        }

        public String getComponentName() {
            return componentName;
        }

        public String getClient() {
            return client;
        }

        public String getContext() {
            return context;
        }

        public String getProxy() {
            return proxy;
        }

        public boolean isProxied() {
            return isForwarded();
        }

    }
}
