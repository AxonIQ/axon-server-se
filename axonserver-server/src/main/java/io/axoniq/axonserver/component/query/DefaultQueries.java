package io.axoniq.axonserver.component.query;

import io.axoniq.axonserver.message.query.QueryDefinition;
import io.axoniq.axonserver.message.query.QueryHandler;
import io.axoniq.axonserver.message.query.QueryRegistrationCache;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by Sara Pellegrini on 20/03/2018.
 * sara.pellegrini@gmail.com
 */
public class DefaultQueries implements Iterable<Query> {

    private final QueryRegistrationCache registrationCache;


    public DefaultQueries(QueryRegistrationCache registrationCache) {
        this.registrationCache = registrationCache;
    }

    @Override
    public Iterator<Query> iterator() {
        Map<QueryDefinition, Map<String, Set<QueryHandler>>> all = registrationCache.getAll();
        return all.entrySet().stream()
                  .map(entry -> (Query) new DefaultQuery(entry.getKey(), entry.getValue(), registrationCache.getResponseTypes(entry.getKey())))
                  .iterator();
    }
}
