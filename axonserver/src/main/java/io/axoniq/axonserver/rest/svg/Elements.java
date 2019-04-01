/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest.svg;

import com.google.common.collect.Iterables;
import io.axoniq.axonserver.rest.svg.attribute.Position;

import java.io.PrintWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;

import static java.util.Arrays.asList;

/**
 * Created by Sara Pellegrini on 01/05/2018.
 * sara.pellegrini@gmail.com
 */
public class Elements implements Element, Composite{

    private final Iterable<? extends Element> boxes;

    public <T> Elements(int x, int y, Iterable<T> items, PositionMapping<T> mapping) {
        Collection<Element> boxList = new LinkedList<>();
        Position p = new Position(x,y);
        for (T item : items) {
            Element element = mapping.map(item, p);
            p = p.shift(element.dimension().width() + 20, 0);
            boxList.add(element);
        }
        this.boxes = Collections.unmodifiableCollection(boxList);
    }

    public <T> Elements(Iterable<T> items, Mapping<T> mapping){
        Collection<Element> boxList = new LinkedList<>();
        for (T item : items) {
            boxList.add(mapping.map(item));
        }
        this.boxes = Collections.unmodifiableCollection(boxList);
    }

    @SafeVarargs
    public <E extends Element> Elements(E ... boxes) {
        this(asList(boxes));
    }

    public Elements(Iterable<? extends Element> boxes) {
        this.boxes = boxes;
    }

    @Override
    public void printOn(PrintWriter writer) {
        boxes.forEach(box -> box.printOn(writer));
    }

    public int boxesNumber(){
        return Iterables.size(boxes);
    }

    @Override
    public Iterable<? extends Element> items() {
        return this.boxes;
    }

}
