/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component.query;

import io.axoniq.axonserver.message.query.QueryDefinition;
import io.axoniq.axonserver.message.query.QueryHandler;
import io.axoniq.axonserver.serializer.Media;

import java.util.Map;
import java.util.Set;

/**
 * Created by Sara Pellegrini on 20/03/2018.
 * sara.pellegrini@gmail.com
 */
public class DefaultQuery implements Query{

    private final QueryDefinition definition;

    private final Map<String, Set<QueryHandler<?>>> handlers;
    private final Set<String> responseTypes;

    public DefaultQuery(QueryDefinition definition,
                        Map<String, Set<QueryHandler<?>>> handlers, Set<String> responseTypes) {
        this.definition = definition;
        this.handlers = handlers;
        this.responseTypes = responseTypes;
    }


    @Override
    public Boolean belongsToComponent(String component) {
        return handlers.entrySet().stream().anyMatch(
                entry -> component.equals(entry.getKey())
                        && entry.getValue() != null
                        && !entry.getValue().isEmpty()
        );
    }

    @Override
    public boolean belongsToContext(String context) {
        return definition.getContext().equals(context);
    }

    @Override
    public void printOn(Media media) {
        media.with("name", definition.getQueryName())
             .withStrings( "responseTypes", responseTypes);
    }
}
