/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest.svg.mapping;

import io.axoniq.axonserver.rest.svg.BoxRegistry;
import io.axoniq.axonserver.rest.svg.Fonts;
import io.axoniq.axonserver.rest.svg.Mapping;
import io.axoniq.axonserver.rest.svg.TextLine;
import io.axoniq.axonserver.rest.svg.attribute.Position;
import io.axoniq.axonserver.rest.svg.attribute.StyleClass;
import io.axoniq.axonserver.rest.svg.decorator.Hidden;
import io.axoniq.axonserver.rest.svg.element.Rectangle;
import io.axoniq.axonserver.rest.svg.element.TextBox;
import io.axoniq.axonserver.topology.AxonServerNode;

import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * Created by Sara Pellegrini on 01/05/2018.
 * sara.pellegrini@gmail.com
 */
public class AxonServerPopupMapping implements Mapping<AxonServer> {

    private final BoxRegistry<String> hubRegistry;

    private final Fonts fonts;

    public AxonServerPopupMapping(BoxRegistry<String> hubRegistry, Fonts fonts) {
        this.hubRegistry = hubRegistry;
        this.fonts = fonts;
    }

    public Hidden map(AxonServer hub) {
        AxonServerNode node = hub.node();
        List<String> lines = new ArrayList<>();
        lines.add("Hostname: " + node.getHostName());
        lines.add("Grpc Port: " + node.getGrpcPort());
        lines.add("Internal Hostname: " + node.getInternalHostName());
        lines.add("Internal Grpc Port: " + node.getGrpcInternalPort());
        lines.add("Http Port: " + node.getHttpPort());
        if (!hub.tags().isEmpty()) {
            lines.add("Tags:");
            hub.tags().forEach((key, value) -> lines.add("- " + key + "=" + value));
        }
        List<TextLine> textLines = lines.stream().map(text -> new TextLine(text, fonts.popup(), StyleClass.POPUP))
                                        .collect(toList());
        Rectangle r = hubRegistry.get(node.getName()).rectangle();
        Position position = r.position().shift(10, r.height() - 5);
        TextBox content = new TextBox(textLines, position, StyleClass.POPUP, new TextBox.Left(5), 10);
        return new Hidden(node.getName() + "-details", content, StyleClass.POPUP);
    }
}
