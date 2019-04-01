/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.query;

import io.axoniq.axonserver.message.ClientIdentification;

import java.util.NavigableSet;
import java.util.Objects;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Marc Gathier
 */
public class RoundRobinQueryHandlerSelector implements QueryHandlerSelector {
    private final ConcurrentMap<QueryDefinitionComponent, ClientIdentification> lastClientMap = new ConcurrentHashMap<>();

    @Override
    public ClientIdentification select(QueryDefinition queryDefinition, String componentName, NavigableSet<ClientIdentification> queryHandlers) {
        if( queryHandlers.isEmpty()) return null;
        QueryDefinitionComponent key = new QueryDefinitionComponent(queryDefinition, componentName);
        ClientIdentification last = lastClientMap.get(key);
        if( last == null) {
            last = queryHandlers.first();
            lastClientMap.put(key, last);
            return last;
        }

        SortedSet<ClientIdentification> tail = queryHandlers.tailSet(last, false);
        if( tail.isEmpty()) {
            last = queryHandlers.first();
        } else  {
            last = tail.first();
        }
        lastClientMap.put(key, last);
        return last;
    }

    private static class QueryDefinitionComponent {
        private final QueryDefinition queryDefinition;
        private final String componentName;

        private QueryDefinitionComponent(QueryDefinition queryDefinition, String componentName) {
            this.queryDefinition = queryDefinition;
            this.componentName = componentName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            QueryDefinitionComponent that = (QueryDefinitionComponent) o;
            return Objects.equals(queryDefinition, that.queryDefinition) &&
                    Objects.equals(componentName, that.componentName);
        }

        @Override
        public int hashCode() {

            return Objects.hash(queryDefinition, componentName);
        }
    }
}
