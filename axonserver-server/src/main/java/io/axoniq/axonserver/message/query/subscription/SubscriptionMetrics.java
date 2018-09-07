package io.axoniq.axonserver.message.query.subscription;

import io.axoniq.axonserver.serializer.Printable;

/**
 * Created by Sara Pellegrini on 18/06/2018.
 * sara.pellegrini@gmail.com
 */
public interface SubscriptionMetrics extends Printable {

    Long totalCount();

    Long activesCount();

    Long updatesCount();

}
