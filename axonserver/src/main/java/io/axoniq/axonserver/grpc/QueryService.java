package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.DispatchEvents;
import io.axoniq.axonserver.SubscriptionEvents;
import io.axoniq.axonserver.SubscriptionQueryEvents.SubscriptionQueryResponseReceived;
import io.axoniq.axonserver.TopologyEvents.QueryHandlerDisconnected;
import io.axoniq.axonserver.grpc.query.*;
import io.axoniq.axonserver.message.query.DirectQueryHandler;
import io.axoniq.axonserver.message.query.QueryDispatcher;
import io.axoniq.axonserver.message.query.QueryResponseConsumer;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * @author Marc Gathier
 */
@Service("QueryService")
public class QueryService extends QueryServiceGrpc.QueryServiceImplBase implements AxonServerClientService {

    private final QueryDispatcher queryDispatcher;
    private final ContextProvider contextProvider;
    private final ApplicationEventPublisher eventPublisher;
    private final Logger logger = LoggerFactory.getLogger(QueryService.class);
    private final Set<GrpcQueryDispatcherListener> dispatcherListenerSet = new CopyOnWriteArraySet<>();

    @Value("${axoniq.axonserver.query-threads:1}")
    private final int processingThreads = 1;


    public QueryService(QueryDispatcher queryDispatcher, ContextProvider contextProvider, ApplicationEventPublisher eventPublisher) {
        this.queryDispatcher = queryDispatcher;
        this.contextProvider = contextProvider;
        this.eventPublisher = eventPublisher;
    }

    @PreDestroy
    public void cleanup() {
        dispatcherListenerSet.forEach(GrpcFlowControlledDispatcherListener::cancel);
        dispatcherListenerSet.clear();
    }

    @Override
    public StreamObserver<QueryProviderOutbound> openStream(
            StreamObserver<QueryProviderInbound> inboundStreamObserver) {
        String context = contextProvider.getContext();

        SendingStreamObserver<QueryProviderInbound> wrappedQueryProviderInboundObserver = new SendingStreamObserver<>(
                inboundStreamObserver);

        return new ReceivingStreamObserver<QueryProviderOutbound>(logger) {
            private volatile GrpcQueryDispatcherListener listener;
            private volatile String client;

            @Override
            protected void consume(QueryProviderOutbound queryProviderOutbound) {
                switch (queryProviderOutbound.getRequestCase()) {
                    case SUBSCRIBE:
                        if (client == null) {
                            client = queryProviderOutbound.getSubscribe().getClientId();
                        }
                        eventPublisher.publishEvent(
                                new SubscriptionEvents.SubscribeQuery(context,
                                                                      queryProviderOutbound
                                                                              .getSubscribe(),
                                                                      new DirectQueryHandler(
                                                                              wrappedQueryProviderInboundObserver,
                                                                              queryProviderOutbound
                                                                                      .getSubscribe()
                                                                                      .getClientId(),
                                                                              queryProviderOutbound
                                                                                      .getSubscribe()
                                                                                      .getComponentName())));
                        break;
                    case UNSUBSCRIBE:
                        if (client != null) {
                            eventPublisher.publishEvent(
                                    new SubscriptionEvents.UnsubscribeQuery(context,
                                                                            queryProviderOutbound
                                                                                    .getUnsubscribe(),
                                                                            false));
                        }
                        break;
                    case FLOW_CONTROL:
                        if (this.listener == null) {
                            listener = new GrpcQueryDispatcherListener(queryDispatcher,
                                                                       queryProviderOutbound.getFlowControl()
                                                                                            .getClientId(),
                                                                       wrappedQueryProviderInboundObserver, processingThreads);
                            dispatcherListenerSet.add(listener);
                        }
                        listener.addPermits(queryProviderOutbound.getFlowControl().getPermits());
                        break;

                    case QUERY_RESPONSE:
                        queryDispatcher.handleResponse(queryProviderOutbound.getQueryResponse(), client, false);
                        break;

                    case QUERY_COMPLETE:
                        queryDispatcher.handleComplete(queryProviderOutbound.getQueryComplete().getRequestId(),
                                                                          client,
                                                                          false);
                        break;
                    case SUBSCRIPTION_QUERY_RESPONSE:
                        SubscriptionQueryResponse response = queryProviderOutbound.getSubscriptionQueryResponse();
                        eventPublisher.publishEvent(new SubscriptionQueryResponseReceived(response));
                        break;
                    case REQUEST_NOT_SET:
                        break;
                }
            }

            @Override
            protected String sender() {
                return client;
            }

            @Override
            public void onError(Throwable cause) {
                logger.warn("{}: Error on connection from subscriber - {}", client, cause.getMessage());

                cleanup();
            }

            private void cleanup() {
                eventPublisher.publishEvent(new QueryHandlerDisconnected(context, client));
                if (listener != null) {
                    dispatcherListenerSet.remove(listener);
                    listener.cancel();
                }
            }

            @Override
            public void onCompleted() {
                cleanup();

                try {
                    inboundStreamObserver.onCompleted();
                } catch( RuntimeException cause) {
                    logger.warn("{}: Error completing connection to subscriber - {}", client, cause.getMessage());
                }

            }
        };
    }

    @Override
    public void query(QueryRequest request, StreamObserver<QueryResponse> responseObserver) {
        if( logger.isTraceEnabled()) logger.trace("{}: Received query: {}", request.getClientId(), request);
        GrpcQueryResponseConsumer responseConsumer = new GrpcQueryResponseConsumer(responseObserver);
        eventPublisher.publishEvent(new DispatchEvents.DispatchQuery(contextProvider.getContext(), request,
                                                                     responseConsumer::onNext,
                                                                     result -> responseConsumer.onCompleted(), false));
    }

    @Override
    public StreamObserver<SubscriptionQueryRequest> subscription(
            StreamObserver<SubscriptionQueryResponse> responseObserver) {
        String context = contextProvider.getContext();
        return new SubscriptionQueryRequestTarget(context, responseObserver, eventPublisher);
    }

    private class GrpcQueryResponseConsumer implements QueryResponseConsumer {

        private final SendingStreamObserver<QueryResponse> responseObserver;

        GrpcQueryResponseConsumer(StreamObserver<QueryResponse> responseObserver) {
            this.responseObserver = new SendingStreamObserver<>(responseObserver);
        }

        @Override
        public void onNext(QueryResponse queryResponse) {
            responseObserver.onNext(queryResponse);
        }

        @Override
        public void onCompleted() {
            responseObserver.onCompleted();
        }
    }
}
