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

import java.io.PrintWriter;
import java.util.function.Supplier;

/**
 * Created by Sara Pellegrini on 27/04/2018.
 * sara.pellegrini@gmail.com
 */
public class Clickable implements Element {

    private final Element content;

    private final Supplier<String> onClickJavascript;

    public Clickable(Element content, Supplier<String> onClickJavascript) {
        this.onClickJavascript = onClickJavascript;
        this.content = content;
    }

    @Override
    public void printOn(PrintWriter writer) {
        writer.printf("<g onclick=\"%s\">%n", onClickJavascript.get());
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
