/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.refactoring.ui.svg.mapping;

import io.axoniq.axonserver.refactoring.ui.svg.attribute.Position;
import io.axoniq.axonserver.refactoring.ui.svg.decorator.Clickable;
import io.axoniq.axonserver.refactoring.ui.svg.decorator.DoubleLine;
import io.axoniq.axonserver.refactoring.ui.svg.BoxRegistry;
import io.axoniq.axonserver.refactoring.ui.svg.Element;
import io.axoniq.axonserver.refactoring.ui.svg.Fonts;
import io.axoniq.axonserver.refactoring.ui.svg.PositionMapping;
import io.axoniq.axonserver.refactoring.ui.svg.TextLine;
import io.axoniq.axonserver.refactoring.ui.svg.element.TextBox;
import io.axoniq.axonserver.refactoring.ui.svg.jsfunction.ShowDetail;

import java.util.List;

import static java.util.Arrays.asList;

/**
 * Created by Sara Pellegrini on 01/05/2018.
 * sara.pellegrini@gmail.com
 */
public class ApplicationBoxMapping implements PositionMapping<Application> {

    public static final String CLIENT = "client";
    private final BoxRegistry<String> hubNodes;

    private final Fonts fonts;

    public ApplicationBoxMapping(BoxRegistry<String> hubNodes, Fonts fonts) {
        this.hubNodes = hubNodes;
        this.fonts = fonts;
    }

    @Override
    public Element map(Application item, Position position) {
        List<TextLine> lines = asList(new TextLine("Application", fonts.type(), "type"),
                                          new TextLine(item.name(), fonts.component(), "component"),
                                          new TextLine(item.instancesString(), fonts.client(), CLIENT));
        ShowDetail showDetail = new ShowDetail(item.component(), CLIENT, item.context(), item.name());
        TextBox client = new TextBox(lines, position, CLIENT);
        item.connectedHubNodes().forEach(hubNode -> client.connectTo(hubNodes.get(hubNode), "applicationToHub"));
        return new Clickable(new DoubleLine(client, item.instances()>1), showDetail);
    }
}
