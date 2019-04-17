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
public class Text implements Printable {

    private final String text;

    private final Position position;

    private final StyleClass styleClass;

    public Text(String text, Position position, String styleClass) {
        this(text, position, new StyleClass(styleClass));
    }

    public Text(String text, Position position, StyleClass styleClass) {
        this.text = text;
        this.position = position;
        this.styleClass = styleClass;
    }

    @Override
    public void printOn(PrintWriter writer) {
        writer.printf("<text ");
        position.printOn(writer);
        styleClass.printOn(writer);
        writer.printf(">%s</text>\n", text);
    }

}
