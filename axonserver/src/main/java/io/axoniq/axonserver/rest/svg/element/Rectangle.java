/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest.svg.element;

import io.axoniq.axonserver.rest.svg.Element;
import io.axoniq.axonserver.rest.svg.attribute.Dimension;
import io.axoniq.axonserver.rest.svg.attribute.Position;
import io.axoniq.axonserver.rest.svg.attribute.StyleClass;

import java.io.PrintWriter;

/**
 * Created by Sara Pellegrini on 27/04/2018.
 * sara.pellegrini@gmail.com
 */
public class Rectangle implements Element {

    private final Position coordinates;

    private final Dimension size;

    private final StyleClass styleClass;

    public Rectangle(Position coordinates, Dimension size, StyleClass styleClass) {
        this.coordinates = coordinates;
        this.size = size;
        this.styleClass = styleClass;
    }

    @Override
    public void printOn(PrintWriter writer) {
        writer.printf("<rect ");
        coordinates.printOn(writer);
        size.printOn(writer);
        styleClass.printOn(writer);
        writer.println("rx=\"5\" ry=\"5\"/>");
    }

    public int x() {
        return this.coordinates.x();
    }

    public int y() {
        return this.coordinates.y();
    }

    public int width() {
        return this.size.width();
    }

    public int height() {
        return this.size.height();
    }

    public Rectangle shift(int xOffset, int yOffset){
        return new Rectangle(this.coordinates.shift(xOffset, yOffset), this.size, this.styleClass);
    }

    @Override
    public Position position() {
        return coordinates;
    }

    @Override
    public Dimension dimension() {
        return size;
    }
}
