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
import io.axoniq.axonserver.rest.svg.element.Box;

import java.io.PrintWriter;

/**
 * Created by Sara Pellegrini on 27/04/2018.
 * sara.pellegrini@gmail.com
 */
public class DoubleLine implements Element {

    private final Box delegate;

    private final boolean enabled;

    public DoubleLine(Box delegate, boolean enabled) {
        this.delegate = delegate;
        this.enabled = enabled;
    }

    @Override
    public void printOn(PrintWriter writer) {
        if (enabled) {
            delegate.rectangle().shift(5, 5).printOn(writer);
        }
        delegate.printOn(writer);
    }

    @Override
    public Position position() {
        return delegate.position();
    }

    @Override
    public Dimension dimension() {
        return delegate.dimension().increase(5,5);
    }
}
