package io.axoniq.axonserver.refactoring.messaging.query.api;

import io.axoniq.axonserver.refactoring.client.instance.Client;
import io.axoniq.axonserver.refactoring.messaging.api.ContextAware;
import reactor.core.publisher.Flux;

/**
 * @author Sara Pellegrini
 * @since
 */
public interface QueryHandler extends ContextAware {

    @Override
    default String context() {
        return definition().context();
    }

    QueryDefinition definition();

    Client client();

    Flux<QueryResponse> handle(Query query);
}
