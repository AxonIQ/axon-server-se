package io.axoniq.axonserver.cluster.election;

import reactor.core.publisher.Mono;


/**
 * @author Sara Pellegrini
 * @since 4.1
 */
public interface Election {

    Mono<Result> result();

    interface Result {

        boolean won();

        boolean goAway();

        String cause();

    }

}
