/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest.svg.jsfunction;

import com.google.common.collect.Lists;

import java.util.function.Supplier;

/**
 * Created by Sara Pellegrini on 30/04/2018.
 * sara.pellegrini@gmail.com
 */
public class ShowDetail implements Supplier<String> {

    private final String popupName;

    private final String nodeType;

    private final Iterable<String> contexts;

    private final String title;

    public ShowDetail(String popupName, String nodeType, Iterable<String> contexts, String title) {
        this.popupName = popupName;
        this.nodeType = nodeType;
        this.contexts = contexts;
        this.title = title;
    }

    @Override
    public String get() {
        return String.format("showArea(event,'%s', '%s', '%s', '%s')", popupName, nodeType, this.contexts == null || Lists.newArrayList(contexts.iterator()).size() > 1 ? null : contexts.iterator().next() , title);
    }
}
