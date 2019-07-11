/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest.svg.attribute;

import io.axoniq.axonserver.rest.svg.Printable;

import java.io.PrintWriter;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class StyleClass implements Printable {
    public static final String POPUP = "popup";
    public static final String SERVER_TO_SERVER = "hubToHub";
    public static final String AXONSERVER = "axonserver";
    public static final String TYPE = "type";
    public static final String DOWN = "down";
    public static final String CURRENT = "current";
    public static final String STORAGE = "axondb";
    public static final String MASTER = "master";
    public static final String CLIENT = "client";
    public static final String ADMIN_LEADER = "admin";

    private final String styleClass;

    public StyleClass(String styleClass) {
        this.styleClass = styleClass;
    }

    @Override
    public void printOn(PrintWriter writer) {
        writer.printf("class=\"%s\"",styleClass);
    }

    @Override
    public String toString() {
        return styleClass;
    }
}
