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
public class StringLiteral implements PipelineEntry {

    private final String literal;

    public StringLiteral(String literal) {
        this.literal = literal.substring(1, literal.length()-1).replace("\\\"", "\"");
    }

    @Override
    public String operator() {
        return "string-literal";
    }

    @Override
    public List<QueryElement> getParameters() {
        return Collections.emptyList();
    }

    public String getLiteral() {
        return literal;
    }

    @Override
    public Optional<String> alias() {
        return Optional.of(literal);
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
    public List<String> getIdentifiers() {
        return Collections.emptyList();
    }

    @Override
    public String toString() {
        return "\"" + literal.replace("\"", "\\\"") + "\"";
    }

}
