/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.applicationevents;

import io.axoniq.axonserver.message.ClientIdentification;

/**
 * Set of events raised when application connect to or disconnect from Axon Server.
 * @author Marc Gathier
 */
public class TopologyEvents {

    public abstract static class TopologyBaseEvent {
        private final boolean forwarded;

        protected TopologyBaseEvent(boolean forwarded) {
            this.forwarded = forwarded;
        }

        public boolean isForwarded() {
            return forwarded;
        }

    }

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

    /**
     * It should be published any time AxonServer doesn't receive any heartbeat from a client
     * for a period of time greater then the set timeout.
     */
    public static class ApplicationInactivityTimeout {

        private final ClientIdentification clientIdentification;

        private final String componentName;

        /**
         * Creates an {@link ApplicationInactivityTimeout} event.
         *
         * @param clientIdentification the client identifier
         * @param componentName        the client component name
         */
        public ApplicationInactivityTimeout(ClientIdentification clientIdentification, String componentName) {
            this.clientIdentification = clientIdentification;
            this.componentName = componentName;
        }

        /**
         * Returns the client identifier.
         * @return the client identifier.
         */
        public ClientIdentification clientIdentification() {
            return clientIdentification;
        }

        /**
         * Returns the component name.
         * @return the component name.
         */
        public String componentName() {
            return componentName;
        }
    }
}
