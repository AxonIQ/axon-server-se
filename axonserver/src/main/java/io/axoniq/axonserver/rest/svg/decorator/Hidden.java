/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest.svg.decorator;

import io.axoniq.axonserver.rest.svg.Element;
import io.axoniq.axonserver.rest.svg.attribute.Dimension;
import io.axoniq.axonserver.rest.svg.attribute.Position;
import io.axoniq.axonserver.rest.svg.attribute.StyleClass;

import java.io.PrintWriter;

/**
 * Created by Sara Pellegrini on 27/04/2018.
 * sara.pellegrini@gmail.com
 */
public class Hidden implements Element {

    private final String id;

    private final Element content;

    private final StyleClass styleClass;

    public Hidden(String id, Element content, String styleClass) {
        this.id = id;
        this.content = content;
        this.styleClass = new StyleClass(styleClass);
    }

    @Override
    public void printOn(PrintWriter writer) {
        writer.printf("<g id=\"%s\" visibility=\"hidden\"", id);
        styleClass.printOn(writer);
        writer.printf(">%n");
        content.printOn(writer);
        writer.printf("</g>%n");
    }

    @Override
    public Position position() {
        return content.position();
    }

    @Override
    public Dimension dimension() {
        return content.dimension();
    }
}
