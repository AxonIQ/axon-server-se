/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.applicationevents.SubscriptionEvents.SubscribeQuery;
import io.axoniq.axonserver.applicationevents.SubscriptionEvents.UnsubscribeQuery;
import io.axoniq.axonserver.applicationevents.SubscriptionQueryEvents.SubscriptionQueryResponseReceived;
import io.axoniq.axonserver.applicationevents.TopologyEvents.QueryHandlerDisconnected;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.ExceptionUtils;
import io.axoniq.axonserver.grpc.query.QueryProviderInbound;
import io.axoniq.axonserver.grpc.query.QueryProviderOutbound;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import io.axoniq.axonserver.grpc.query.QueryServiceGrpc;
import io.axoniq.axonserver.grpc.query.QuerySubscription;
import io.axoniq.axonserver.grpc.query.SubscriptionQuery;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryResponse;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.message.query.DirectQueryHandler;
import io.axoniq.axonserver.message.query.QueryDispatcher;
import io.axoniq.axonserver.message.query.QueryHandler;
import io.axoniq.axonserver.message.query.QueryResponseConsumer;
import io.axoniq.axonserver.topology.Topology;
import io.axoniq.axonserver.util.StreamObserverUtils;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.PreDestroy;

/**
 * GRPC service to handle query bus requests from Axon Application
 * Client can sent two requests:
 * query: sends a singe query to AxonServer
 * openStream: used by application providing query handlers, maintains an open bi directional connection between the
 * application and AxonServer
 *
 * @author Marc Gathier
 */
@Service("QueryService")
public class QueryService extends QueryServiceGrpc.QueryServiceImplBase implements AxonServerClientService {

    private final Topology topology;
    private final QueryDispatcher queryDispatcher;
    private final ContextProvider contextProvider;
    private final ClientIdRegistry clientIdRegistry;
    private final ApplicationEventPublisher eventPublisher;
    private final Logger logger = LoggerFactory.getLogger(QueryService.class);
    private final Map<ClientStreamIdentification, GrpcQueryDispatcherListener> dispatcherListeners = new ConcurrentHashMap<>();
    private final InstructionAckSource<QueryProviderInbound> instructionAckSource;

    @Value("${axoniq.axonserver.query-threads:1}")
    private int processingThreads = 1;


    public QueryService(Topology topology, QueryDispatcher queryDispatcher, ContextProvider contextProvider,
                        ClientIdRegistry clientIdRegistry,
                        ApplicationEventPublisher eventPublisher,
                        @Qualifier("queryInstructionAckSource")
                                InstructionAckSource<QueryProviderInbound> instructionAckSource) {
        this.topology = topology;
        this.queryDispatcher = queryDispatcher;
        this.contextProvider = contextProvider;
        this.clientIdRegistry = clientIdRegistry;
        this.eventPublisher = eventPublisher;
        this.instructionAckSource = instructionAckSource;
    }

    @PreDestroy
    public void cleanup() {
        dispatcherListeners.forEach((client, listener) -> listener.cancel());
        dispatcherListeners.clear();
    }


    @Override
    public StreamObserver<QueryProviderOutbound> openStream(
            StreamObserver<QueryProviderInbound> inboundStreamObserver) {
        String context = contextProvider.getContext();
        SendingStreamObserver<QueryProviderInbound> wrappedQueryProviderInboundObserver = new SendingStreamObserver<>(
                inboundStreamObserver);

        return new ReceivingStreamObserver<QueryProviderOutbound>(logger) {
            private final AtomicReference<String> clientIdRef = new AtomicReference<>();
            private final AtomicReference<GrpcQueryDispatcherListener> listener = new AtomicReference<>();
            private final AtomicReference<ClientStreamIdentification> clientRef = new AtomicReference<>();
            private final AtomicReference<QueryHandler<QueryProviderInbound>> queryHandler = new AtomicReference<>();

            @Override
            protected void consume(QueryProviderOutbound queryProviderOutbound) {
                switch (queryProviderOutbound.getRequestCase()) {
                    case SUBSCRIBE:
                        instructionAckSource.sendSuccessfulAck(queryProviderOutbound.getInstructionId(),
                                                               wrappedQueryProviderInboundObserver);
                        QuerySubscription subscription = queryProviderOutbound.getSubscribe();
                        checkInitClient(subscription.getClientId(), subscription.getComponentName());
                        String clientStreamId = clientRef.get().getClientStreamId();
                        logger.debug("{}: Subscribe Query {} for {}",
                                     context,
                                     subscription.getQuery(),
                                     subscription.getClientId());
                        SubscribeQuery subscribeQuery = new SubscribeQuery(context,
                                                                           clientStreamId,
                                                                           subscription,
                                                                           queryHandler.get());
                        eventPublisher.publishEvent(
                                subscribeQuery);
                        break;
                    case UNSUBSCRIBE:
                        instructionAckSource.sendSuccessfulAck(queryProviderOutbound.getInstructionId(),
                                                               wrappedQueryProviderInboundObserver);
                        if (clientRef.get() != null) {
                            QuerySubscription unsubscribe = queryProviderOutbound.getUnsubscribe();
                            UnsubscribeQuery unsubscribeQuery = new UnsubscribeQuery(context,
                                                                                     clientRef.get()
                                                                                              .getClientStreamId(),
                                                                                     unsubscribe,
                                                                                     false);
                            eventPublisher.publishEvent(unsubscribeQuery);
                        }
                        break;
                    case FLOW_CONTROL:
                        instructionAckSource.sendSuccessfulAck(queryProviderOutbound.getInstructionId(),
                                                               wrappedQueryProviderInboundObserver);
                        flowControl(queryProviderOutbound.getFlowControl());
                        break;
                    case QUERY_RESPONSE:
                        instructionAckSource.sendSuccessfulAck(queryProviderOutbound.getInstructionId(),
                                                               wrappedQueryProviderInboundObserver);
                        queryDispatcher.handleResponse(queryProviderOutbound.getQueryResponse(),
                                                       clientRef.get().getClientStreamId(),
                                                       clientIdRef.get(),
                                                       false);
                        break;
                    case QUERY_COMPLETE:
                        instructionAckSource.sendSuccessfulAck(queryProviderOutbound.getInstructionId(),
                                                               wrappedQueryProviderInboundObserver);
                        queryDispatcher.handleComplete(queryProviderOutbound.getQueryComplete().getRequestId(),
                                                       clientRef.get().getClientStreamId(),
                                                       clientIdRef.get(),
                                                       false);
                        break;
                    case SUBSCRIPTION_QUERY_RESPONSE:
                        instructionAckSource.sendSuccessfulAck(queryProviderOutbound.getInstructionId(),
                                                               wrappedQueryProviderInboundObserver);
                        SubscriptionQueryResponse response = queryProviderOutbound.getSubscriptionQueryResponse();
                        eventPublisher.publishEvent(new SubscriptionQueryResponseReceived(response, () ->
                                wrappedQueryProviderInboundObserver
                                        .onNext(unsubscribeMessage(response.getSubscriptionIdentifier()))));
                        break;
                    case ACK:
                        InstructionAck ack = queryProviderOutbound.getAck();
                        if (isUnsupportedInstructionErrorResult(ack)) {
                            logger.warn("Unsupported instruction sent to the client {} of context {}.",
                                        clientRef.get().getClientStreamId(),
                                        context);
                        } else {
                            logger.trace("Received instruction ack from the client {} of context {}. Result {}.",
                                         clientRef.get().getClientStreamId(),
                                         context,
                                         ack);
                        }
                        break;
                    default:
                        instructionAckSource.sendUnsupportedInstruction(queryProviderOutbound.getInstructionId(),
                                                                        topology.getMe().getName(),
                                                                        wrappedQueryProviderInboundObserver);
                        break;
                }
            }

            private void flowControl(FlowControl flowControl) {
                initClientReference(flowControl.getClientId());
                if (listener.compareAndSet(null, new GrpcQueryDispatcherListener(queryDispatcher,
                                                                                 clientRef.get().toString(),
                                                                                 wrappedQueryProviderInboundObserver,
                                                                                 processingThreads))) {
                    dispatcherListeners.put(clientRef.get(), listener.get());
                }
                listener.get().addPermits(flowControl.getPermits());
            }

            private void initClientReference(String clientId) {
                String clientStreamId = clientId + "." + UUID.randomUUID().toString();
                if (clientRef.compareAndSet(null, new ClientStreamIdentification(context, clientStreamId))) {
                    clientIdRegistry.register(clientStreamId, clientId, ClientIdRegistry.ConnectionType.QUERY);
                }
                clientIdRef.compareAndSet(null, clientId);
            }

            private void checkInitClient(String clientId, String componentName) {
                initClientReference(clientId);
                queryHandler.compareAndSet(null,
                                           new DirectQueryHandler(wrappedQueryProviderInboundObserver, clientRef.get(),
                                                                  componentName, clientId));
            }

            @Override
            protected String sender() {
                return clientRef.toString();
            }

            @Override
            public void onError(Throwable cause) {
                if (!ExceptionUtils.isCancelled(cause)) {
                    logger.warn("{}: Error on connection from subscriber - {}", clientRef, cause.getMessage());
                }

                cleanup();
            }

            private void cleanup() {
                if (clientRef.get() != null) {
                    String clientStreamId = clientRef.get().getClientStreamId();
                    String clientId = this.clientIdRef.get();
                    clientIdRegistry.unregister(clientStreamId, ClientIdRegistry.ConnectionType.QUERY);
                    eventPublisher.publishEvent(new QueryHandlerDisconnected(context,
                                                                             clientId,
                                                                             clientStreamId));
                }
                if (listener.get() != null) {
                    dispatcherListeners.remove(clientRef.get());
                    listener.get().cancel();
                }
                StreamObserverUtils.complete(inboundStreamObserver);
            }

            @Override
            public void onCompleted() {
                cleanup();
            }
        };
    }

    private boolean isUnsupportedInstructionErrorResult(InstructionAck instructionResult) {
        return instructionResult.hasError()
                && instructionResult.getError().getErrorCode().equals(ErrorCode.UNSUPPORTED_INSTRUCTION.getCode());
    }

    @NotNull
    private QueryProviderInbound unsubscribeMessage(String subscriptionIdentifier) {
        return QueryProviderInbound.newBuilder()
                                   .setSubscriptionQueryRequest(
                                           SubscriptionQueryRequest.newBuilder()
                                                                   .setUnsubscribe(SubscriptionQuery.newBuilder()
                                                                                                    .setSubscriptionIdentifier(
                                                                                                            subscriptionIdentifier)))
                                   .build();
    }

    @Override
    public void query(QueryRequest request, StreamObserver<QueryResponse> responseObserver) {
        if (logger.isTraceEnabled()) {
            logger.trace("{}: Received query: {}", request.getClientId(), request.getQuery());
        }
        GrpcQueryResponseConsumer responseConsumer = new GrpcQueryResponseConsumer(responseObserver);
        queryDispatcher.query(new SerializedQuery(contextProvider.getContext(), request),
                              responseConsumer::onNext,
                              result -> responseConsumer.onCompleted());
    }

    @Override
    public StreamObserver<SubscriptionQueryRequest> subscription(
            StreamObserver<SubscriptionQueryResponse> responseObserver) {
        String context = contextProvider.getContext();
        return new SubscriptionQueryRequestTarget(context, responseObserver, eventPublisher);
    }

    public Set<GrpcQueryDispatcherListener> listeners() {
        return new HashSet<>(dispatcherListeners.values());
    }

    private class GrpcQueryResponseConsumer implements QueryResponseConsumer {

        private final SendingStreamObserver<QueryResponse> responseObserver;

        GrpcQueryResponseConsumer(StreamObserver<QueryResponse> responseObserver) {
            this.responseObserver = new SendingStreamObserver<>(responseObserver);
        }

        @Override
        public void onNext(QueryResponse queryResponse) {
            try {
                responseObserver.onNext(queryResponse);
            } catch (Exception ex) {
                logger.debug("Sending response failed", ex);
            }
        }

        @Override
        public void onCompleted() {
            responseObserver.onCompleted();
        }
    }

    /**
     * Completes the query stream to the specified client.
     *
     * @param clientId                   the unique identifier of the client instance
     * @param clientStreamIdentification the unique identifier of the query stream
     */
    public void completeStream(String clientId, ClientStreamIdentification clientStreamIdentification) {
        if (dispatcherListeners.containsKey(clientStreamIdentification)) {
            dispatcherListeners.remove(clientStreamIdentification).cancelAndCompleteStream();
            logger.debug("Query Stream closed for client: {}", clientStreamIdentification);
            eventPublisher.publishEvent(new QueryHandlerDisconnected(clientStreamIdentification.getContext(),
                                                                     clientId,
                                                                     clientStreamIdentification.getClientStreamId()));
        }
    }
}
