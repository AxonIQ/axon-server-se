package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.message.query.QueryDispatcher;
import io.axoniq.axonserver.message.query.WrappedQuery;
import org.slf4j.Logger;

/**
 * Validates a query request before sending it to the query handler.
 * @author Marc Gathier
 */
public interface QueryRequestValidator {

    /**
     * Validates a query request. Checks timeout to verify that the request still needs to be sent.
     * @param message the query to handle
     * @param queryDispatcher the target for the query
     * @param logger logger to log messages to
     * @return serialized query message to send if message is valid, null if message is not valid.
     */
    default SerializedQuery validate(WrappedQuery message, QueryDispatcher queryDispatcher, Logger logger) {
        SerializedQuery serializedQuery = message.queryRequest();
        QueryRequest request = serializedQuery.query();
        long messageTimeout = message.timeout();
        long remainingTime =  messageTimeout - System.currentTimeMillis();
        if(remainingTime < 0) {
            logger.debug("Timeout for message: {} - {}ms", request.getMessageIdentifier(), remainingTime);
            queryDispatcher.removeFromCache(request.getMessageIdentifier());
            return null;
        } else {
            logger.debug("Remaining time for message: {} - {}ms", request.getMessageIdentifier(), remainingTime);
        }

        if( messageTimeout - remainingTime > 10) {
            serializedQuery = serializedQuery.withTimeout(remainingTime);
        }
        return serializedQuery;
    }
}
