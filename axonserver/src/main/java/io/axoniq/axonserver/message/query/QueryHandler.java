package io.axoniq.axonserver.message.query;


import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.axoniq.axonserver.message.FlowControlQueues;
import io.grpc.stub.StreamObserver;

import java.util.Objects;

/**
 * @author Marc Gathier
 */
public abstract class QueryHandler<T>  {
    private final String clientName;
    private final String componentName;
    protected final StreamObserver<T> streamObserver;

    protected QueryHandler(StreamObserver<T> streamObserver, String clientName, String componentName) {
        this.clientName = clientName;
        this.streamObserver = streamObserver;
        this.componentName = componentName;
    }

    public abstract void dispatch(SubscriptionQueryRequest query);

    public String getClientName() {
        return clientName;
    }

    public String getComponentName() {
        return componentName;
    }

    public String toString() {
        return clientName;
    }

    public void enqueue(String context, QueryRequest request, FlowControlQueues<WrappedQuery> queryQueue, long timeout) {
        queryQueue.put(clientName, new WrappedQuery(context, request, timeout));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QueryHandler<?> that = (QueryHandler<?>) o;
        return Objects.equals(clientName, that.clientName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientName);
    }
}
