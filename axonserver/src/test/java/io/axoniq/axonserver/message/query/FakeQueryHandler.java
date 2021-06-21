package io.axoniq.axonserver.message.query;

import io.axoniq.axonserver.grpc.SerializedQuery;
import io.axoniq.axonserver.grpc.query.SubscriptionQueryRequest;
import io.axoniq.axonserver.message.ClientStreamIdentification;

import java.util.concurrent.atomic.AtomicInteger;

public class FakeQueryHandler extends QueryHandler {

    private final AtomicInteger requestCount = new AtomicInteger();

    public FakeQueryHandler(ClientStreamIdentification clientStreamIdentification, String componentName,
                            String client) {
        super(clientStreamIdentification, componentName, client);
    }

    @Override
    public void dispatch(SubscriptionQueryRequest query) {
        requestCount.incrementAndGet();
    }

    @Override
    public void dispatch(SerializedQuery request, long timeout) {
        requestCount.incrementAndGet();
    }

    public int requests() {
        return requestCount.get();
    }
}
