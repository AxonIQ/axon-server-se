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
public class FunctionName implements PipelineEntry {

    private final String identifier;

    public FunctionName(String identifier) {
        this.identifier = identifier;
    }

    public String getIdentifier() {
        return identifier;
    }

    @Override
    public void add(PipelineEntry o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public PipelineEntry get(int idx) {
        return null;
    }

    @Override
    public String toString() {
        return identifier;
    }

    @Override
    public List<String> getIdentifiers() {
        return Collections.singletonList(identifier);
    }

    @Override
    public String operator() {
        return "functionName";
    }

    @Override
    public List<? extends QueryElement> getParameters() {
        return Collections.emptyList();
    }

    @Override
    public String getLiteral() {
        return identifier;
    }

    @Override
    public Optional<String> alias() {
        return Optional.of(identifier);
    }
}
