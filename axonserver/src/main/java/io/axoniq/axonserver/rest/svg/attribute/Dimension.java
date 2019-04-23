/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest.svg.attribute;

import io.axoniq.axonserver.rest.svg.Printable;

import java.io.PrintWriter;

/**
 * Created by Sara Pellegrini on 27/04/2018.
 * sara.pellegrini@gmail.com
 */
public class Dimension implements Printable {

    private final int width;

    private final int height;

    public Dimension(int width, int height) {
        this.width = width;
        this.height = height;
    }

    public Dimension increase(int width, int height){
        return new Dimension(this.width + width, this.height + height);
    }

    @Override
    public void printOn(PrintWriter writer) {
        writer.print("width=\""+width+"\" height=\""+height+"\" ");
    }

    public int width() {
        return width;
    }

    public int height() {
        return height;
    }
}
