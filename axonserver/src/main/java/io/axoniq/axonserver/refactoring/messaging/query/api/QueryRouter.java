package io.axoniq.axonserver.refactoring.messaging.query.api;

import org.springframework.security.core.Authentication;
import reactor.core.publisher.Flux;

/**
 * @author Sara Pellegrini
 * @since
 */
public interface QueryRouter {

    Flux<QueryResponse> dispatch(Authentication authentication, Query query);
}
