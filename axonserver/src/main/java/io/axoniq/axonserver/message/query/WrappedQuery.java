package io.axoniq.axonserver.message.query;

import io.axoniq.axonserver.ProcessingInstructionHelper;
import io.axoniq.axonserver.grpc.query.QueryRequest;

/**
 * @author Marc Gathier
 */
public class WrappedQuery {
    private final QueryRequest queryRequest;
    private final String context;
    private final long timeout;
    private final long priority;

    public WrappedQuery(String context, QueryRequest queryRequest, long timeout) {
        this.context = context;
        this.queryRequest = queryRequest;
        this.timeout = timeout;
        this.priority = ProcessingInstructionHelper.priority(queryRequest.getProcessingInstructionsList());
    }

    public QueryRequest queryRequest() {
        return queryRequest;
    }

    public long timeout() {
        return timeout;
    }

    public long priority() {
        return priority;
    }

    public String context() {
        return context;
    }
}
