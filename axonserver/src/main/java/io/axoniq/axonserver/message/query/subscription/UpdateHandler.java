package io.axoniq.axonserver.message.query.subscription;

import io.axoniq.axonserver.grpc.query.SubscriptionQueryResponse;
/**
 * Created by Sara Pellegrini on 03/05/2018.
 * sara.pellegrini@gmail.com
 */
public interface UpdateHandler {

    void onSubscriptionQueryResponse(SubscriptionQueryResponse response);

}
