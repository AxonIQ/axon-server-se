package io.axoniq.axonserver.message.query;

import io.axoniq.axonserver.KeepNames;
import io.axoniq.axonserver.grpc.query.QuerySubscription;

import java.util.Objects;

/**
 * Author: marc
 */
@KeepNames
public class QueryDefinition {
    private final String context;
    private final String queryName;

    public QueryDefinition(String context, String queryName) {
        this.context = context;
        this.queryName = queryName;
    }

    public QueryDefinition(String context, QuerySubscription subscribe) {
        this.queryName = subscribe.getQuery();
        this.context = context;
    }

    public String getQueryName() {
        return queryName;
    }

    public String getContext() {
        return context;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryDefinition that = (QueryDefinition) o;
        return Objects.equals(context, that.context) &&
                Objects.equals(queryName, that.queryName);
    }

    @Override
    public int hashCode() {

        return Objects.hash(context, queryName);
    }

    @Override
    public String toString() {
        return "QueryDefinition{" +
                "context='" + context + '\'' +
                ", queryName='" + queryName + '\'' +
                '}';
    }
}
