/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.query;

import java.util.Objects;

/**
 * @author Marc Gathier
 */
public class QueryDefinition {
    private final String context;
    private final String queryName;

    public QueryDefinition(String context, String queryName) {
        this.context = context;
        this.queryName = queryName;
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
