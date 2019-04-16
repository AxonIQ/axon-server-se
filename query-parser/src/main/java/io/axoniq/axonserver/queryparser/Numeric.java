/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.queryparser;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * @author Marc Gathier
 * @since 4.0
 */
public class Numeric implements PipelineEntry {

    private String value;

    public Numeric(String value) {
        this.value = value.trim();
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public String operator() {
        return "numeric-literal";
    }

    @Override
    public List<QueryElement> getParameters() {
        return Collections.emptyList();
    }

    @Override
    public String getLiteral() {
        return value;
    }

    @Override
    public Optional<String> alias() {
        return Optional.empty();
    }


    @Override
    public void add(PipelineEntry pipelineEntry) {
        throw new UnsupportedOperationException("Cannot add subexpressions to literals");
    }

    @Override
    public PipelineEntry get(int idx) {
        throw new UnsupportedOperationException("Cannot add subexpressions to literals");
    }

    @Override
    public List<String> getIdentifiers() {
        return Collections.emptyList();
    }
}
