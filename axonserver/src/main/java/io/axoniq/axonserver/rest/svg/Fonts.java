/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest.svg;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.util.function.Supplier;

/**
 * Created by Sara Pellegrini on 30/04/2018.
 * sara.pellegrini@gmail.com
 */
public class Fonts {

    private final FontResource type;
    private final FontResource eventStore;
    private final FontResource client;
    private final FontResource component;
    private final FontResource messaging;
    private final FontResource popup;

    public Fonts() {
        BufferedImage img = new BufferedImage(10, 10, BufferedImage.TRANSLUCENT);
        img.createGraphics();
        Graphics2D g = (Graphics2D) img.getGraphics();
        g.setRenderingHint(RenderingHints.KEY_FRACTIONALMETRICS, RenderingHints.VALUE_FRACTIONALMETRICS_ON);
        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        String montserratItalic = "static/fonts/Montserrat-Italic.ttf";
        String latoBold = "static/fonts/lato-bold.ttf";
        String montserrat = "static/fonts/Montserrat-Regular.ttf";
        this.type = new FontResource(g, montserratItalic, 10f);
        this.eventStore = new FontResource(g, latoBold, 12f);
        this.client = new FontResource(g, montserratItalic, 9f);
        this.component = new FontResource(g, latoBold, 12f);
        this.messaging = new FontResource(g, latoBold, 12f);
        this.popup = new FontResource(g, montserrat, 10f);
    }


    public FontMetricsWrapper type() {
        return type.get();
    }

    public FontMetricsWrapper eventStore() {
        return eventStore.get();
    }

    public FontMetricsWrapper client() {
        return client.get();
    }

    public FontMetricsWrapper component() {
        return component.get();
    }

    public FontMetricsWrapper messaging() {
        return messaging.get();
    }

    public FontMetricsWrapper popup() {
        return popup.get();
    }


    private class FontResource implements Supplier<FontMetricsWrapper> {

        private final FontMetricsWrapper font;

        FontResource(Graphics2D g, String resource, float size) {
            this.font = getFont(g, resource, size);
        }

        private FontMetricsWrapper getFont(Graphics2D g, String resource, float size) {
            try {
                ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
                Font wrappedFont = Font.createFont(Font.TRUETYPE_FONT, classLoader.getResourceAsStream(resource));
                return new FontMetricsWrapper(g.getFontMetrics(wrappedFont.deriveFont(size)));
            } catch (Exception e) {
                return FontMetricsWrapper.INSTANCE;
            }
        }

        @Override
        public FontMetricsWrapper get() {
            return font;
        }
    }
}
