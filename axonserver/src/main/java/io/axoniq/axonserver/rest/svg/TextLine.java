/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest.svg;

/**
 * Created by Sara Pellegrini on 27/04/2018.
 * sara.pellegrini@gmail.com
 */
public class TextLine {

    private final String text;
    private final FontMetricsWrapper font;
    private final String styleClass;

    public TextLine(String text, FontMetricsWrapper font, String styleClass) {
        this.text = text;
        this.font = font;
        this.styleClass = styleClass;
    }

    public int width() {
        return font.stringWidth(text);
    }

    public int height() {return font.getHeight();}

    public int ascent(){ return  font.getAscent();}

    public String styleClass(){return styleClass;}

    public String text() {return text;}


}
