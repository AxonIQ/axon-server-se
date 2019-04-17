/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest.svg.element;

import io.axoniq.axonserver.rest.svg.Printable;
import io.axoniq.axonserver.rest.svg.attribute.Position;
import io.axoniq.axonserver.rest.svg.attribute.StyleClass;

import java.io.PrintWriter;

/**
 * Created by Sara Pellegrini on 27/04/2018.
 * sara.pellegrini@gmail.com
 */
public class Line implements Printable {

    private final Position from;

    private final Position to;

    private final StyleClass styleClass;

    public Line(Position from, Position to, StyleClass styleClass) {
        this.from = from;
        this.to = to;
        this.styleClass = styleClass;
    }

    @Override
    public void printOn(PrintWriter writer) {
        writer.printf("<line x1=\"%d\" y1=\"%d\" x2=\"%d\" y2=\"%d\" ", from.x(), from.y(), to.x(), to.y());
        styleClass.printOn(writer);
        writer.printf("stroke-width=\"2\"/>%n");
    }
}
