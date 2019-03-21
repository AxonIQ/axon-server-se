package io.axoniq.axonserver.applicationevents;

import io.axoniq.axonserver.KeepNames;
import io.axoniq.axonserver.message.ClientIdentification;

/**
 * Set of events raised when application connect to or disconnect from Axon Server.
 * @author Marc Gathier
 */
public class TopologyEvents {

    @KeepNames
    public abstract static class TopologyBaseEvent {
        private final boolean forwarded;

        protected TopologyBaseEvent(boolean forwarded) {
            this.forwarded = forwarded;
        }

        public boolean isForwarded() {
            return forwarded;
        }

    }

    @KeepNames
    public static class ApplicationConnected extends TopologyBaseEvent {
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

        public ClientIdentification clientIdentification() {
            return new ClientIdentification(context, client);
        }
    }

    @KeepNames
    public static class ApplicationDisconnected extends TopologyBaseEvent {
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

        public ClientIdentification clientIdentification() {
            return new ClientIdentification(context, client);
        }

    }

    @KeepNames
    public static class CommandHandlerDisconnected extends TopologyBaseEvent {
        private final String context;
        private final String client;

        public CommandHandlerDisconnected(String context, String client, boolean proxied) {
            super(proxied);
            this.context = context;
            this.client = client;
        }

        public CommandHandlerDisconnected(String context, String client
        ) {
            this(context, client, false);
        }

        public String getClient() {
            return client;
        }

        public String getContext() {
            return context;
        }

        public boolean isProxied() {
            return isForwarded();
        }

        public ClientIdentification clientIdentification() {
            return new ClientIdentification(context, client);
        }
    }

    @KeepNames
    public static class QueryHandlerDisconnected extends TopologyBaseEvent {
        private final String context;
        private final String client;

        public QueryHandlerDisconnected(String context, String client, boolean proxied) {
            super(proxied);
            this.context = context;
            this.client = client;
        }

        public QueryHandlerDisconnected(String context, String client
        ) {
            this(context, client, false);
        }

        public String getClient() {
            return client;
        }

        public String getContext() {
            return context;
        }

        public boolean isProxied() {
            return isForwarded();
        }

        public ClientIdentification clientIdentification() {
            return new ClientIdentification(context, client);
        }

    }
}
