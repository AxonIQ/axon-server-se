/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.queryparser;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author Marc Gathier
 * @since 4.0
 */
public class FunctionExpr extends BaseQueryElement {
    private String alias;

    @Override
    public String toString() {
        return children.get(0) + "( " +
                String.join(", ", children.stream().skip(1).map(Object::toString).collect(Collectors.toList()))
                + ")" + (alias == null ? "" : " as " + alias);
    }

    @Override
    public List<String> getIdentifiers() {
        List<String> identifiers = new ArrayList<>();
        children.stream().skip(1).forEach(pipelineEntry -> identifiers.addAll(pipelineEntry.getIdentifiers()));
        return identifiers;
    }

    @Override
    public String operator() {
        return String.valueOf(children.get(0));
    }

    @Override
    public List<? extends QueryElement> getParameters() {
        return children.subList(1, children.size());
    }

    public Optional<String> alias() {
        return Optional.ofNullable(alias);
    }

    public void setAlias(String alias) {
       this.alias = alias;
    }

    public void setOperator(String operator) {
        children.add(0, new FunctionName(operator));
    }

    public int size() {
        return children.size();
    }
}
