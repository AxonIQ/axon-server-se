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

/**
 * @author Marc Gathier
 * @since 4.0
 */
public abstract class BaseQueryElement implements PipelineEntry{

    protected final List<PipelineEntry> children = new ArrayList<>();

    @Override
    public void add(PipelineEntry pipelineEntry) {
        children.add(pipelineEntry);
    }

    @Override
    public PipelineEntry get(int idx) {
        return children.get(idx);
    }

    @Override
    public List<String> getIdentifiers() {
        List<String> identifiers = new ArrayList<>();
        children.forEach(pipelineEntry -> identifiers.addAll(pipelineEntry.getIdentifiers()));
        return identifiers;
    }

    @Override
    public Optional<String> alias() {
        return Optional.empty();
    }

    @Override
    public String getLiteral() {
        return operator();
    }

    public int size() {
        return children.size();
    }
}
