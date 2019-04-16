/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.queryparser;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Container for a parsed query.
 * @author Marc Gathier
 * @since 4.0
 */
public class Query extends BaseQueryElement {

    @Override
    public String toString() {
        return String.join(" | ", children.stream().map(PipelineEntry::toString).collect(Collectors.toList()));
    }

    @Override
    public String operator() {
        return "query";
    }

    @Override
    public List<? extends QueryElement> getParameters() {
        return children;
    }

    @Override
    public String getLiteral() {
        return toString();
    }

    public long getStartTime() {
        return children.stream().filter(pipelineEntry -> pipelineEntry instanceof TimeConstraint)
                                                                 .map(TimeConstraint.class::cast)
                                                                 .map(tc -> tc.getStart())
                                                                 .max(Long::compareTo)
                                                                 .orElse(0L);
    }

    public void addDefaultLimit(long limit) {
        if(! hasLimit()) {
            FunctionExpr limitExpr = new FunctionExpr();
            limitExpr.add(new Identifier("limit"));
            limitExpr.add(new Numeric(String.valueOf(limit)));
            add(limitExpr);
        }
    }

    private boolean hasLimit() {
        for( PipelineEntry entry: children) {
            if( entry.operator().equalsIgnoreCase("limit")) return true;
        }
        return false;
    }


}
