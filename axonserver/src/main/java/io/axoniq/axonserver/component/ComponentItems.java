/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.component;

import io.axoniq.axonserver.util.StringUtils;

import java.util.Iterator;

import static java.util.stream.StreamSupport.stream;

/**
 * Created by Sara Pellegrini on 19/03/2018.
 * sara.pellegrini@gmail.com
 */
public class ComponentItems<I extends ComponentItem> implements Iterable<I> {

    private final String component;

    private final String context;
    private final Iterable<I> delegate;

    public ComponentItems(String component, String context, Iterable<I> delegate) {
        this.component = component;
        this.context = context;
        this.delegate = delegate;
    }

    @Override
    public Iterator<I> iterator() {
        return stream(delegate.spliterator(), false)
                .filter(item -> item.belongsToComponent(component))
                .filter(item -> StringUtils.isEmpty(context) || context.equals("null") || item.belongsToContext(context))
                .iterator();
    }
}
