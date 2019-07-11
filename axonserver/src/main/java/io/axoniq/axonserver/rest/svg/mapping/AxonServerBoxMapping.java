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
import io.axoniq.axonserver.rest.svg.Element;
import io.axoniq.axonserver.rest.svg.Fonts;
import io.axoniq.axonserver.rest.svg.PositionMapping;
import io.axoniq.axonserver.rest.svg.TextLine;
import io.axoniq.axonserver.rest.svg.attribute.Position;
import io.axoniq.axonserver.rest.svg.attribute.StyleClass;
import io.axoniq.axonserver.rest.svg.decorator.Clickable;
import io.axoniq.axonserver.rest.svg.element.AxonServerGroup;
import io.axoniq.axonserver.rest.svg.element.Box;
import io.axoniq.axonserver.rest.svg.element.Store;
import io.axoniq.axonserver.rest.svg.element.TextBox;
import io.axoniq.axonserver.rest.svg.jsfunction.ShowDetail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public class AxonServerBoxMapping implements PositionMapping<AxonServer>, BoxRegistry<String> {

    private final Map<String, Box> boxMap = new HashMap<>();

    private final boolean showContexts;

    private final String currentNode;
    private final Fonts fonts;

    public AxonServerBoxMapping(boolean showContexts, String currentNode, Fonts fonts) {
        this.showContexts = showContexts;
        this.currentNode = currentNode;
        this.fonts = fonts;
    }

    @Override
    public Element map(AxonServer axonServer, Position position) {
        List<TextLine> lines = new ArrayList<>();
        lines.add(new TextLine("AxonServer", fonts.type(), StyleClass.TYPE));
        String name = axonServer.node().getName();
        String extraClass = (axonServer.isActive() ? StyleClass.SERVER_TO_SERVER :
                StyleClass.SERVER_TO_SERVER + " " + StyleClass.DOWN);
        if (name.equals(currentNode)) {
            extraClass += " " + StyleClass.CURRENT;
        }
        if (showContexts && axonServer.isAdminLeader()) {
            extraClass += " " + StyleClass.ADMIN_LEADER;
        }
        String nodeType = StyleClass.AXONSERVER + " " + extraClass;
        lines.add(new TextLine(name, fonts.messaging(), nodeType));
        Iterable<String> contextNames = axonServer.contexts();
        if (showContexts) {
            contextNames.forEach(context -> lines.add(new TextLine(context, fonts.client(), StyleClass.CLIENT)));
        }
        String popupName = name + "-details";
        ShowDetail showDetail = new ShowDetail(popupName, nodeType, null, null);
        TextBox tmpContent = new TextBox(lines, position, nodeType);
        int contentHeight = tmpContent.dimension().height() + 20;
        Position dbPosition = new Position(position.x(), position.y() + contentHeight);
        List<Store> stores = new ArrayList<>();
        for (Storage axonDB : axonServer.storage()) {
            Store newStore = new Store(showContexts ? new TextLine(axonDB.context(),
                                                                   fonts.messaging(),
                                                                   StyleClass.AXONSERVER) : null,
                                       dbPosition,
                                       new StyleClass(
                                               StyleClass.STORAGE + (axonDB.master() ? " " + StyleClass.MASTER : "")));
            stores.add(newStore);
            dbPosition = new Position(dbPosition.x() + newStore.dimension().width() + 10, dbPosition.y());
        }
        int storesWidth = dbPosition.x() - position.x() - 10;
        if (storesWidth < tmpContent.dimension().width()) {
            stores = stores.stream().map(s -> s.move((tmpContent.dimension().width() - storesWidth) / 2, 0)).collect(
                    Collectors.toList());
        }

        int x = position.x() + Math.max(tmpContent.dimension().width(), storesWidth) / 2
                - tmpContent.dimension().width() / 2;
        TextBox content = new TextBox(lines, new Position(x, position.y()), nodeType);
        Element element = new Clickable(content, showDetail);
        boxMap.put(name, content);
        return new AxonServerGroup(element, stores, position);
    }

    @Override
    public Box get(String key) {
        return boxMap.get(key);
    }
}
