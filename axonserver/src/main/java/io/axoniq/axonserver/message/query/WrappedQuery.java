package io.axoniq.axonserver.message.query;

import io.axoniq.axonserver.ProcessingInstructionHelper;
import io.axoniq.axonserver.grpc.SerializedQuery;
import io.axoniq.axonserver.message.ClientIdentification;

/**
 * Author: marc
 */
public class WrappedQuery {
    private final SerializedQuery queryRequest;
    private final ClientIdentification client;
    private final long timeout;
    private final long priority;

    public WrappedQuery(ClientIdentification client, SerializedQuery queryRequest, long timeout) {
        this.queryRequest = queryRequest;
        this.timeout = timeout;
        this.client = client;
        this.priority = ProcessingInstructionHelper.priority(queryRequest.query().getProcessingInstructionsList());
    }

    public SerializedQuery queryRequest() {
        return queryRequest;
    }

    public long timeout() {
        return timeout;
    }

    public long priority() {
        return priority;
    }

    public ClientIdentification client() {
        return client;
    }
}
