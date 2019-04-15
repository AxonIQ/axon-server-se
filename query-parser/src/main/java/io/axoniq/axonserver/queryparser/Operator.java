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
 * Author: marc
 */
public class Operator implements PipelineEntry {

    private final String operator;

    public Operator(String operator) {
        this.operator = operator;
    }

    public String getOperator() {
        return operator;
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
        return operator;
    }

    @Override
    public List<String> getIdentifiers() {
        return Collections.emptyList();
    }

    @Override
    public String operator() {
        return operator;
    }

    @Override
    public List<? extends QueryElement> getParameters() {
        return Collections.emptyList();
    }

    @Override
    public String getLiteral() {
        return operator;
    }

    @Override
    public Optional<String> alias() {
        return null;
    }
}
