package io.axoniq.axonserver.message.query;


import io.axoniq.axonserver.grpc.SerializedQuery;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.axoniq.axonserver.message.ClientIdentification;
import io.axoniq.axonserver.message.FlowControlQueues;
import io.grpc.stub.StreamObserver;

import java.util.Objects;

/**
 * Author: marc
 */
public abstract class QueryHandler<T>  {
    private final ClientIdentification client;
    private final String componentName;
    protected final StreamObserver<T> streamObserver;

    protected QueryHandler(StreamObserver<T> streamObserver, ClientIdentification client, String componentName) {
        this.client = client;
        this.streamObserver = streamObserver;
        this.componentName = componentName;
    }

    public abstract void dispatch(SubscriptionQueryRequest query);

    public ClientIdentification getClient() {
        return client;
    }

    public String getComponentName() {
        return componentName;
    }

    public String queueName() {
        return client.toString();
    }

    public String toString() {
        return client.toString();
    }

    public void enqueue(SerializedQuery request, FlowControlQueues<WrappedQuery> queryQueue, long timeout) {
        queryQueue.put(queueName(), new WrappedQuery(client, request, timeout));
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
        return Objects.equals(client, that.client);
    }

    @Override
    public int hashCode() {
        return Objects.hash(client);
    }

    public String getClientId() {
        return client.getClient();
    }
}
