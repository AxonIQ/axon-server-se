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
import io.axoniq.axonserver.rest.svg.Elements;
import io.axoniq.axonserver.rest.svg.attribute.Dimension;
import io.axoniq.axonserver.rest.svg.attribute.Position;
import io.axoniq.axonserver.rest.svg.attribute.StyleClass;
import io.axoniq.axonserver.rest.svg.element.Rectangle;

import java.io.PrintWriter;

/**
 * Created by Sara Pellegrini on 02/05/2018.
 * sara.pellegrini@gmail.com
 */
public class Grouped implements Element {

    private final Rectangle rectangle;

    private final Elements content;

    public Grouped(Elements content, String styleClass) {
        this.content = content;
        this.rectangle = new Rectangle(content.position().shift(-5,-5),
                                       content.dimension().increase(15, 15),
                                       new StyleClass(styleClass));
    }

    @Override
    public void printOn(PrintWriter writer) {
        if( content.boxesNumber() > 1) {
            rectangle.printOn(writer);
        }
        content.printOn(writer);
    }

    @Override
    public Position position() {
        return rectangle.position();
    }

    @Override
    public Dimension dimension() {
        return rectangle.dimension();
    }
}
