/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.applicationevents;

import io.axoniq.axonserver.grpc.ClientContext;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import org.springframework.context.ApplicationEvent;

/**
 * Set of events raised when application connect to or disconnect from Axon Server.
 *
 * @author Marc Gathier
 */
public class TopologyEvents {

    private TopologyEvents() {
    }

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

        /**
         * The unique identifier of the platform long living stream opened by the client.
         */
        private final String clientStreamId;

        /**
         * The unique identifier of the client that has been connected to Axon Server.
         */
        private final String clientId;
        private final String proxy;

        public ApplicationConnected(String context,
                                    String componentName,
                                    String clientStreamId,
                                    String clientId,
                                    String proxy) {
            super(proxy != null);
            this.context = context;
            this.componentName = componentName;
            this.clientStreamId = clientStreamId;
            this.clientId = clientId;
            this.proxy = proxy;
        }

        public ApplicationConnected(String context, String componentName, String clientStreamId) {
            this(context, componentName, clientStreamId, clientStreamId, null);
        }

        public String getComponentName() {
            return componentName;
        }

        /**
         * Returns the unique identifier of the platform long living stream opened by the client.
         *
         * @return the unique identifier of the platform long living stream opened by the client.
         */
        public String getClientStreamId() {
            return clientStreamId;
        }

        /**
         * Returns the unique identifier of the client that has been connected to Axon Server.
         *
         * @return the unique identifier of the client that has been connected to Axon Server.
         */
        public String getClientId() {
            return clientId;
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

        public ClientStreamIdentification clientStreamIdentification() {
            return new ClientStreamIdentification(context, clientStreamId);
        }
    }

    public static class ApplicationDisconnected extends TopologyBaseEvent {

        private final String context;
        private final String componentName;

        /**
         * The unique identifier of the platform long living stream opened by the client.
         */
        private final String clientStreamId;

        /**
         * The unique identifier of the client that has been connected to Axon Server.
         */
        private final String clientId;
        private final String proxy;
        private final String reason;

        public ApplicationDisconnected(String context,
                                       String componentName,
                                       String clientStreamId,
                                       String clientId,
                                       String proxy,
                                       String reason) {
            super(proxy != null);
            this.context = context;
            this.componentName = componentName;
            this.clientStreamId = clientStreamId;
            this.clientId = clientId;
            this.proxy = proxy;
            this.reason = reason;
        }

        public ApplicationDisconnected(String context,
                                       String componentName,
                                       String clientStreamId,
                                       String reason
        ) {
            this(context, componentName, clientStreamId, clientStreamId, null, reason);
        }

        public String getComponentName() {
            return componentName;
        }

        /**
         * Returns the unique identifier of the platform long living stream opened by the client.
         *
         * @return the unique identifier of the platform long living stream opened by the client.
         */
        public String getClientStreamId() {
            return clientStreamId;
        }

        /**
         * Returns the unique identifier of the client that has been connected to Axon Server.
         *
         * @return the unique identifier of the client that has been connected to Axon Server.
         */
        public String getClientId() {
            return clientId;
        }

        public String getContext() {
            return context;
        }

        public String getProxy() {
            return proxy;
        }

        public String getReason() {
            return reason;
        }

        public boolean isProxied() {
            return isForwarded();
        }

        public ClientStreamIdentification clientIdentification() {
            return new ClientStreamIdentification(context, clientStreamId);
        }
    }

    public static class CommandHandlerDisconnected extends TopologyBaseEvent {

        private final String context;

        /**
         * The unique identifier of the client that has been connected to Axon Server.
         */
        private final String clientId;

        /**
         * The unique identifier of the command long living stream opened by the client.
         */
        private final String clientStreamId;

        public CommandHandlerDisconnected(String context,
                                          String clientId,
                                          String clientStreamId,
                                          boolean proxied) {
            super(proxied);
            this.context = context;
            this.clientId = clientId;
            this.clientStreamId = clientStreamId;
        }

        public CommandHandlerDisconnected(String context, String clientId, String clientStreamId
        ) {
            this(context, clientId, clientStreamId, false);
        }

        /**
         * Returns the unique identifier of the command long living stream opened by the client.
         *
         * @return the unique identifier of the command long living stream opened by the client.
         */
        public String getClientStreamId() {
            return clientStreamId;
        }

        /**
         * Returns the unique identifier of the client that has been connected to Axon Server.
         *
         * @return the unique identifier of the client that has been connected to Axon Server.
         */
        public String getClientId() {
            return clientId;
        }

        public String getContext() {
            return context;
        }

        public boolean isProxied() {
            return isForwarded();
        }

        public ClientStreamIdentification clientIdentification() {
            return new ClientStreamIdentification(context, clientStreamId);
        }
    }

    public static class QueryHandlerDisconnected extends TopologyBaseEvent {

        private final String context;

        /**
         * The unique identifier of the client that has been connected to Axon Server.
         */
        private final String clientId;

        /**
         * The unique identifier of the query long living stream opened by the client.
         */
        private final String clientStreamId;

        public QueryHandlerDisconnected(String context,
                                        String clientId,
                                        String clientStreamId,
                                        boolean proxied) {
            super(proxied);
            this.context = context;
            this.clientId = clientId;
            this.clientStreamId = clientStreamId;
        }

        public QueryHandlerDisconnected(String context, String clientId, String clientStreamId
        ) {
            this(context, clientId, clientStreamId, false);
        }

        /**
         * Returns the unique identifier of the query long living stream opened by the client.
         *
         * @return the unique identifier of the query long living stream opened by the client.
         */
        public String getClientStreamId() {
            return clientStreamId;
        }

        /**
         * Returns the unique identifier of the client that has been connected to Axon Server.
         *
         * @return the unique identifier of the client that has been connected to Axon Server.
         */
        public String getClientId() {
            return clientId;
        }

        public String getContext() {
            return context;
        }

        public boolean isProxied() {
            return isForwarded();
        }

        public ClientStreamIdentification clientIdentification() {
            return new ClientStreamIdentification(context, clientStreamId);
        }
    }

    /**
     * It should be published any time AxonServer doesn't receive any heartbeat from a client
     * for a period of time greater then the set timeout.
     */
    public static class ApplicationInactivityTimeout {

        private final ClientStreamIdentification clientStreamIdentification;

        private final String componentName;

        private final ClientContext client;

        /**
         * Creates an {@link ApplicationInactivityTimeout} event.
         *
         * @param clientStreamIdentification the client identifier
         * @param componentName              the client component name
         * @param client                     the client id and context
         */
        public ApplicationInactivityTimeout(ClientStreamIdentification clientStreamIdentification,
                                            String componentName, ClientContext client) {
            this.clientStreamIdentification = clientStreamIdentification;
            this.componentName = componentName;
            this.client = client;
        }

        /**
         * Returns the client identifier.
         *
         * @return the client identifier.
         */
        public ClientStreamIdentification clientStreamIdentification() {
            return clientStreamIdentification;
        }

        /**
         * Returns the component name.
         *
         * @return the component name.
         */
        public String componentName() {
            return componentName;
        }

        /**
         * Returns the unique identifier of the client
         *
         * @return the unique identifier of the client
         */
        public ClientContext client() {
            return client;
        }
    }

    public static class ApplicationReconnectRequested  {
        private final String context;
        private final String componentName;

        /**
         * The unique identifier of the client that has been connected to Axon Server.
         */
        private final String clientId;
        private final String reason;

        public ApplicationReconnectRequested(String context, String componentName,
                                             String clientId, String reason) {
            this.context = context;
            this.componentName = componentName;
            this.clientId = clientId;
            this.reason = reason;
        }

        public String context() {
            return context;
        }

        public String componentName() {
            return componentName;
        }

        public String clientId() {
            return clientId;
        }

        public String reason() {
            return reason;
        }
    }
}
