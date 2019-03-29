/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest.svg.element;

import io.axoniq.axonserver.rest.svg.TextLine;
import io.axoniq.axonserver.rest.svg.attribute.Dimension;
import io.axoniq.axonserver.rest.svg.attribute.Position;
import io.axoniq.axonserver.rest.svg.attribute.StyleClass;

import java.io.PrintWriter;
import java.util.Collection;
import java.util.LinkedList;

/**
 * @author Marc Gathier
 */
public class Store implements Box {
    private static final Dimension STORE_SIZE = new Dimension(64, 32);
    private final TextLine name;
    private final Position coordinates;
    private final Dimension actualSize;
    private final StyleClass styleClass;
    private final Collection<Line> links = new LinkedList<>();
    private final TextBox.HorizontalAlign align;

    public Store(TextLine name, Position coordinates, StyleClass styleClass) {
        this.name = name;
        this.align = new TextBox.Center();
        this.coordinates = coordinates;
        this.styleClass = styleClass;
        if( name == null) {
            this.actualSize = STORE_SIZE;
        } else {
            this.actualSize = new Dimension(Math.max(STORE_SIZE.width(), name.width()), STORE_SIZE.height() + name.height() + 5);
        }
    }
    @Override
    public Position position() {
        return coordinates;
    }

    @Override
    public Dimension dimension() {
        return actualSize;
    }

    @Override
    public void printOn(PrintWriter writer) {
        writer.printf("<g transform=\"translate(%d, %d) scale(2, 1)\">", coordinates.x() + (actualSize.width() - STORE_SIZE.width())/2, coordinates.y());
        writer.printf("<path d=\"M16,0 C9,0,2,2,2,6.5 v19 C2,30,9,32,16,32 c7,0,14-2,14-6.5 v-19 C30,2,22,0,16,0\" class=\"%s\"/>", styleClass);
        writer.write("<path d=\"M28,25.5 c0,2-5,4.5-12,4.5 c-6,0-12-2-12-4.5 v-3 C6,23,11,25,16,25 c4.95,0,10-1,12-3 V25.5z\" fill=\"white\"/>");
        writer.write("<path d=\"M28,19 C28,22,22,24,16,24 c-7,0-12-2-12-4 H4 v-4 C6,18,11,19,16,19 c5,0,10-1,12-3 V19.5z\" fill=\"white\"/>");
        writer.write("<path d=\"M28,13.5 C28,16,23,18,16,18 c-7,0-12-2-12-4 H4 v-3 C7,12,11,13,16,13 c5,0,9-1,12-3 V13.5z\" fill=\"white\"/>");
        writer.write("<path d=\"M16,11 C10,11,4,9,4,7 C4,4,9,2,16,2 c7,0,12,2,12,4.5 C28,9,23,11,16,11z\" fill=\"#CCCCCC\" />");
        writer.write("<circle cx=\"25\" cy=\"26\" fill=\"#333333\" r=\"1\"/>");
        writer.write("<circle cx=\"25\" cy=\"20\" fill=\"#333333\" r=\"1\"/>");
        writer.write("<circle cx=\"25\" cy=\"14\" fill=\"#333333\" r=\"1\"/>");
        writer.println("</g>");

        if( name != null) {
            new Text(name.text(),
                     new Position(coordinates.x() + align.getOffset(name, this),
                                  coordinates.y() + STORE_SIZE.height() + 5 + name.ascent()),
                     name.styleClass()).printOn(writer);
        }
        links.forEach(line -> line.printOn(writer));
    }

    @Override
    public Rectangle rectangle() {
        return new Rectangle(position(), actualSize, styleClass);
    }

    @Override
    public void connectTo(Box to, String lineStyle) {
        links.add(new Line(new Position(rectangle().x() + dimension().width() / 2, rectangle().y() + dimension().height()),
                           new Position(to.rectangle().x() + to.rectangle().width() / 2, to.rectangle().y()),
                           new StyleClass(lineStyle)));

    }

    public Store move(int x, int y) {
        return new Store(name, position().shift(x, y), styleClass);
    }
}
