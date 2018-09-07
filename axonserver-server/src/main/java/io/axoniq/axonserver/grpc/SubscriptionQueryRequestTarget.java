package io.axoniq.axonserver.grpc;

import io.axoniq.axonhub.SubscriptionQuery;
import io.axoniq.axonserver.SubscriptionQueryEvents.SubscriptionQueryCanceled;
import io.axoniq.axonserver.SubscriptionQueryEvents.SubscriptionQueryInitialResultRequested;
import io.axoniq.axonserver.SubscriptionQueryEvents.SubscriptionQueryRequested;
import io.axoniq.axonhub.SubscriptionQueryRequest;
import io.axoniq.axonhub.SubscriptionQueryResponse;
import io.axoniq.axonserver.message.query.subscription.UpdateHandler;
import io.axoniq.axonserver.message.query.subscription.handler.DirectUpdateHandler;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Created by Sara Pellegrini on 30/05/2018.
 * sara.pellegrini@gmail.com
 */
public class SubscriptionQueryRequestTarget extends ReceivingStreamObserver<SubscriptionQueryRequest> {

    private final String context;

    private final FlowControlledStreamObserver<SubscriptionQueryResponse> responseObserver;

    private final ApplicationEventPublisher eventPublisher;

    private final List<SubscriptionQuery> subscriptionQuery;

    private final UpdateHandler updateHandler;

    private final Consumer<Throwable> errorHandler;

    private volatile String client;

    SubscriptionQueryRequestTarget(
            String context, StreamObserver<SubscriptionQueryResponse> responseObserver,
            ApplicationEventPublisher eventPublisher) {
        super(LoggerFactory.getLogger(SubscriptionQueryRequestTarget.class));
        this.context = context;
        this.errorHandler = e -> responseObserver.onError(Status.INTERNAL
                                                                  .withDescription(e.getMessage())
                                                                  .withCause(e)
                                                                  .asRuntimeException());
        this.responseObserver = new FlowControlledStreamObserver<>(responseObserver, errorHandler);
        this.updateHandler = new DirectUpdateHandler(this.responseObserver::onNext);
        this.eventPublisher = eventPublisher;
        this.subscriptionQuery = new ArrayList<>();
    }

    @Override
    protected void consume(SubscriptionQueryRequest message) {
        switch (message.getRequestCase()) {
            case SUBSCRIBE:
                if (client == null) {
                    client = message.getSubscribe().getQueryRequest().getClientId();
                }
                subscriptionQuery.add(message.getSubscribe());
                eventPublisher.publishEvent(new SubscriptionQueryRequested(context,
                                                                           subscriptionQuery.get(0),
                                                                           updateHandler,
                                                                           errorHandler));

                break;
            case GET_INITIAL_RESULT:
                if (subscriptionQuery.isEmpty()){
                    errorHandler.accept(new IllegalStateException("Initial result asked before subscription"));
                    break;
                }
                eventPublisher.publishEvent(new SubscriptionQueryInitialResultRequested(context,
                                                                                        subscriptionQuery.get(0),
                                                                                        updateHandler,
                                                                                        errorHandler));
                break;
            case FLOW_CONTROL:
                responseObserver.addPermits(message.getFlowControl().getNumberOfPermits());
                break;
            case UNSUBSCRIBE:
                if (!subscriptionQuery.isEmpty()) {
                    unsubscribe(subscriptionQuery.get(0));
                }
                break;
        }

    }

    @Override
    protected String sender() {
        return client;
    }

    @Override
    public void onError(Throwable t) {
        if (!subscriptionQuery.isEmpty()) {
            unsubscribe(subscriptionQuery.get(0));
        }
    }

    @Override
    public void onCompleted() {
        if (!subscriptionQuery.isEmpty()) {
            unsubscribe(subscriptionQuery.get(0));
        }
    }

    private void unsubscribe(SubscriptionQuery cancel) {
        subscriptionQuery.remove(cancel);
        eventPublisher.publishEvent(new SubscriptionQueryCanceled(context, cancel));
    }
}
