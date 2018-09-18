package io.axoniq.axonserver.message.query.subscription.handler;

import io.axoniq.axonserver.grpc.query.SubscriptionQueryResponse;
import io.axoniq.axonserver.message.query.subscription.UpdateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Sara Pellegrini on 11/05/2018.
 * sara.pellegrini@gmail.com
 */
public class MissingUpdateHandler implements UpdateHandler {

    private final Logger logger = LoggerFactory.getLogger(MissingUpdateHandler.class);

    @Override
    public void onSubscriptionQueryResponse(SubscriptionQueryResponse response) {
        logger.warn("Cannot find handler for subscriptionId {}. It's not possible to handle SubscriptionQueryResponse.",
                    response.getSubscriptionIdentifier());

    }
}
