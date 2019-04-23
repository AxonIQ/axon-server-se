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
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Marc Gathier
 */
public class AxonServerGroup implements Box {
    private final Rectangle rectangle;
    private final Element axonhub;
    private final List<Store> stores;
    private final Collection<Line> links = new LinkedList<>();


    public AxonServerGroup(Element axonhub, List<Store> stores, Position position) {
        this.axonhub = axonhub;
        this.stores = stores;

        int maxX = axonhub.position().x() + axonhub.dimension().width();
        int maxY = axonhub.position().y() + axonhub.dimension().height();
        for (Store store : stores) {
            maxX = Math.max(maxX, store.position().x() + store.dimension().width());
            maxY = Math.max(maxY, store.position().y() + store.dimension().height());
            links.add(new Line(new Position(axonhub.position().x() + axonhub.dimension().width() / 2, axonhub.position().y() + axonhub.dimension().height()),
                               new Position(store.rectangle().x() + store.rectangle().width() / 2, store.rectangle().y()),
                               new StyleClass("hubToHub")));
        }

        rectangle = new Rectangle(new Position(position.x(), position.y()), new Dimension(maxX - position.x(), maxY - position.y()), null);
    }

    @Override
    public Rectangle rectangle() {
        return rectangle;
    }

    @Override
    public void connectTo(Box to, String lineStyle) {
        links.add(new Line(new Position(rectangle().x() + dimension().width() / 2, rectangle().y() + dimension().height()),
                           new Position(to.rectangle().x() + to.rectangle().width() / 2, to.rectangle().y()),
                           new StyleClass(lineStyle)));

    }

    @Override
    public Position position() {
        return new Position(rectangle.x(), rectangle.y());
    }

    @Override
    public Dimension dimension() {
        return new Dimension(rectangle.width(), rectangle.height());
    }

    @Override
    public void printOn(PrintWriter writer) {
        axonhub.printOn(writer);
        stores.forEach(store -> store.printOn(writer));
        links.forEach(line -> line.printOn(writer));

    }
}
