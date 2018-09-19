package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.ProcessingInstructionHelper;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.message.query.QueryDispatcher;
import io.axoniq.axonserver.message.query.WrappedQuery;
import org.slf4j.Logger;

/**
 * Author: marc
 */
public interface QueryRequestValidator {
    default QueryRequest validate(WrappedQuery message, QueryDispatcher queryDispatcher, Logger logger) {
        QueryRequest request = message.queryRequest();
        long remainingTime =  message.timeout() - System.currentTimeMillis();
        if(remainingTime < 0) {
            logger.debug("Timeout for message: {} - {}ms", request.getMessageIdentifier(), remainingTime);
            queryDispatcher.removeFromCache(request.getMessageIdentifier());
            return null;
        } else {
            logger.debug("Remaining time for message: {} - {}ms", request.getMessageIdentifier(), remainingTime);
        }

        int timeoutIndex = -1;
        for(int i = 0; i < request.getProcessingInstructionsList().size(); i++) {
            if(ProcessingKey.TIMEOUT.equals(request.getProcessingInstructions(i).getKey()) ) {
                timeoutIndex = i;
                break;
            }
        }
        if( timeoutIndex >= 0) {
            request = QueryRequest.newBuilder(request)
                    .removeProcessingInstructions(timeoutIndex)
                    .addProcessingInstructions(ProcessingInstructionHelper.timeout(remainingTime))
                    .build();
        } else {
            request = QueryRequest.newBuilder(request)
                    .addProcessingInstructions(ProcessingInstructionHelper.timeout(remainingTime))
                    .build();
        }
        return request;
    }
}
