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
import io.axoniq.axonserver.rest.svg.TextLine;
import io.axoniq.axonserver.rest.svg.attribute.Dimension;
import io.axoniq.axonserver.rest.svg.attribute.Position;
import io.axoniq.axonserver.rest.svg.attribute.StyleClass;

import java.io.PrintWriter;
import java.util.Collection;
import java.util.LinkedList;

import static java.lang.Math.max;
import static java.util.stream.StreamSupport.stream;

/**
 * Created by Sara Pellegrini on 27/04/2018.
 * sara.pellegrini@gmail.com
 */
public class TextBox implements Box {

    private final Iterable<TextLine> texts;

    private final Position position;

    private final StyleClass styleClass;

    private final HorizontalAlign align;

    private final int widthMargin;

    private final int verticalSpace = 4;

    private final Collection<Line> links = new LinkedList<>();

    public TextBox(Iterable<TextLine> texts, Position position, String styleClass) {
        this(texts, position, styleClass, new Center(), 30);
    }

    public TextBox(Iterable<TextLine> texts, Position position, String styleClass, HorizontalAlign align, int widthMargin) {
        this.position = position;
        this.texts = texts;
        this.styleClass = new StyleClass(styleClass);
        this.align = align;
        this.widthMargin = widthMargin;
    }

    @Override
    public void printOn(PrintWriter writer) {
        rectangle().printOn(writer);
        int x = position.x();
        int y = position.y() + verticalSpace;
        for (TextLine line : texts) {
            new Text(line.text(),
                     new Position(x + align.getOffset(line, this), y + line.ascent()),
                     line.styleClass()).printOn(writer);
            y += line.height() + verticalSpace;
        }
        links.forEach(link -> link.printOn(writer));
    }

    @Override
    public Rectangle rectangle() {
        return new Rectangle(position, new Dimension(width(), height()), styleClass);
    }

    @Override
    public void connectTo(Box to, String lineStyle) {
        links.add(new Line(new Position(rectangle().x() + width() / 2, rectangle().y() + height()),
                           new Position(to.rectangle().x() + to.rectangle().width() / 2, to.rectangle().y()),
                           new StyleClass(lineStyle)));
    }

    private int height() {
        return stream(texts.spliterator(), false).map(e -> e.height()+verticalSpace).reduce(Integer::sum).orElse(0) + 8;
    }

    private int width() {
        return  max(100, stream(texts.spliterator(), false).map(TextLine::width).max(Integer::compareTo).orElse(50) + widthMargin);
    }

    @Override
    public Position position() {
        return position;
    }

    @Override
    public Dimension dimension() {
        return rectangle().dimension();
    }

    public interface HorizontalAlign {

        int getOffset(TextLine line, Element textBox);
    }

    public static class Center implements HorizontalAlign {

        @Override
        public int getOffset(TextLine line, Element textBox) {
            return textBox.dimension().width() / 2 - line.width() / 2;
        }
    }

    public static class Left implements HorizontalAlign {

        private final int leftMargin;

        public Left(int leftMargin) {
            this.leftMargin = leftMargin;
        }

        @Override
        public int getOffset(TextLine line, Element textBox) {
            return leftMargin;
        }
    }
}
