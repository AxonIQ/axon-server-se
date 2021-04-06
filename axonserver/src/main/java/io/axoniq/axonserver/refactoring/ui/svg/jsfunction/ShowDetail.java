/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.refactoring.ui.svg.jsfunction;

import java.util.function.Supplier;

/**
 * Created by Sara Pellegrini on 30/04/2018.
 * sara.pellegrini@gmail.com
 */
public class ShowDetail implements Supplier<String> {

    private final String popupName;

    private final String nodeType;

    private final String context;

    private final String title;

    public ShowDetail(String popupName, String nodeType, String context, String title) {
        this.popupName = popupName;
        this.nodeType = nodeType;
        this.context = context;
        this.title = title;
    }

    @Override
    public String get() {
        return String.format("showArea(event,'%s', '%s', '%s', '%s')", popupName, nodeType, context, title);
    }
}
