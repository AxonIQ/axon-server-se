/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.applicationevents;

import io.axoniq.axonserver.grpc.PlatformService;
import io.axoniq.axonserver.message.ClientIdentification;

/**
 * Set of events raised when application connect to or disconnect from Axon Server.
 * @author Marc Gathier
 */
public class TopologyEvents {

    public abstract static class TopologyBaseEvent {

        private final boolean proxied;

        protected TopologyBaseEvent(boolean proxied) {
            this.proxied = proxied;
        }

        public boolean isProxied() {
            return proxied;
        }
    }

    public abstract static class ApplicationBaseEvent extends TopologyBaseEvent {

        private final ClientIdentification client;

        protected ApplicationBaseEvent(ClientIdentification client, boolean proxied) {
            super(proxied);
            this.client = client;
        }

        public String getClient() {
            return client.getClient();
        }

        public String getContext() {
            return client.getContext();
        }

        public ClientIdentification clientIdentification() {
            return client;
        }
    }

    public static class ApplicationConnected extends ApplicationBaseEvent {

        private final String componentName;
        private final String proxy;

        public ApplicationConnected(ClientIdentification client, String componentName, String proxy) {
            super(client, proxy != null);
            this.componentName = componentName;
            this.proxy = proxy;
        }

        public ApplicationConnected(PlatformService.ClientComponent clientComponent) {
            this(new ClientIdentification(clientComponent.getContext(), clientComponent.getClient()),
                 clientComponent.getComponent());
        }

        public ApplicationConnected(String context, String componentName, String client, String proxy) {
            this(new ClientIdentification(context, client), componentName, proxy);
        }

        public ApplicationConnected(ClientIdentification client, String componentName) {
            this(client, componentName, null);
        }

        public String getComponentName() {
            return componentName;
        }


        public String getProxy() {
            return proxy;
        }
    }

    public static class ApplicationDisconnected extends ApplicationBaseEvent {

        private final String componentName;
        private final String proxy;

        public ApplicationDisconnected(ClientIdentification client, String componentName, String proxy) {
            super(client, proxy != null);
            this.componentName = componentName;
            this.proxy = proxy;
        }

        public ApplicationDisconnected(ClientIdentification client,
                                       String componentName
        ) {
            this(client, componentName, null);
        }

        public ApplicationDisconnected(String context, String componentName, String client, String proxy) {
            this(new ClientIdentification(context, client), componentName, proxy);
        }

        public ApplicationDisconnected(PlatformService.ClientComponent clientComponent) {
            this(new ClientIdentification(clientComponent.getContext(), clientComponent.getClient()),
                 clientComponent.getComponent());
        }

        public String getComponentName() {
            return componentName;
        }

        public String getProxy() {
            return proxy;
        }

    }

    public static class CommandHandlerDisconnected extends ApplicationBaseEvent {

        public CommandHandlerDisconnected(ClientIdentification client, boolean proxied) {
            super(client, proxied);
        }

        public CommandHandlerDisconnected(ClientIdentification client) {
            this(client, false);
        }
    }

    public static class QueryHandlerDisconnected extends ApplicationBaseEvent {

        public QueryHandlerDisconnected(ClientIdentification client, boolean proxied) {
            super(client, proxied);
        }

        public QueryHandlerDisconnected(ClientIdentification client) {
            this(client, false);
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
