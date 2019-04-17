/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest.svg;

import io.axoniq.axonserver.rest.svg.attribute.Dimension;
import io.axoniq.axonserver.rest.svg.attribute.Position;

import static java.util.stream.StreamSupport.stream;

/**
 * Created by Sara Pellegrini on 02/05/2018.
 * sara.pellegrini@gmail.com
 */
public interface Composite extends Element {

    Iterable<? extends Element> items();

    @Override
    default Position position() {
        int x = stream(items().spliterator(), false)
                .map(Element::position)
                .map(Position::x)
                .min(Integer::compareTo).orElse(0);
        int y = stream(items().spliterator(), false)
                .map(Element::position)
                .map(Position::y)
                .min(Integer::compareTo).orElse(0);
        return new Position(x, y);
    }

    @Override
    default Dimension dimension() {
        int lastX = 0;
        int lastY = 0;
        for( Element element : items()) {
            lastX = Math.max(lastX, element.position().x() + element.dimension().width());
            lastY = Math.max(lastY, element.position().y() + element.dimension().height());
        }

        Position p = position();
        return new Dimension(lastX - p.x(), lastY - p.y());
    }
}
