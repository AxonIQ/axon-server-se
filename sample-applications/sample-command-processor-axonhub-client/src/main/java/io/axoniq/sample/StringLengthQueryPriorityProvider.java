package io.axoniq.sample;

import io.axoniq.axonhub.client.query.QueryPriorityCalculator;
import org.axonframework.queryhandling.QueryMessage;

/**
 * Author: marc
 */
public class StringLengthQueryPriorityProvider implements QueryPriorityCalculator {
    @Override
    public int determinePriority(QueryMessage<?, ?> queryMessage) {
        if( queryMessage.getPayload() instanceof String) {
            return 100-String.valueOf(queryMessage.getPayload()).length();
        }
        return 0;
    }
}
