package io.axoniq.axonhub.rest.svg.mapping;

import io.axoniq.axonhub.rest.svg.BoxRegistry;
import io.axoniq.axonhub.rest.svg.Element;
import io.axoniq.axonhub.rest.svg.Fonts;
import io.axoniq.axonhub.rest.svg.PositionMapping;
import io.axoniq.axonhub.rest.svg.TextLine;
import io.axoniq.axonhub.rest.svg.attribute.Position;
import io.axoniq.axonhub.rest.svg.attribute.StyleClass;
import io.axoniq.axonhub.rest.svg.decorator.Clickable;
import io.axoniq.axonhub.rest.svg.element.AxonHubGroup;
import io.axoniq.axonhub.rest.svg.element.Box;
import io.axoniq.axonhub.rest.svg.element.Store;
import io.axoniq.axonhub.rest.svg.element.TextBox;
import io.axoniq.axonhub.rest.svg.jsfunction.ShowDetail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by Sara Pellegrini on 01/05/2018.
 * sara.pellegrini@gmail.com
 */
public class AxonHubBoxMapping implements PositionMapping<AxonHub>, BoxRegistry<String> {

    private final Map<String, Box> boxMap = new HashMap<>();

    private final boolean showContexts;

    private final String currentNode;
    private final Fonts fonts;

    public AxonHubBoxMapping(boolean showContexts, String currentNode, Fonts fonts) {
        this.showContexts = showContexts;
        this.currentNode = currentNode;
        this.fonts = fonts;
    }

    @Override
    public Element map(AxonHub hub, Position position) {
        List<TextLine> lines = new ArrayList<>();
        lines.add(new TextLine("AxonHub", fonts.type(), "type"));
        String name = hub.node().getName();
        String extraClass = (hub.isActive() ? "hubToHub" : "hubToHub down");
        if( name.equals(currentNode)) {
            extraClass += " current";
        }
        String nodeType = "axonhub " + extraClass;
        lines.add(new TextLine(name, fonts.messaging(), nodeType));
        Iterable<String> contextNames = hub.contexts();
        if (showContexts) {
            contextNames.forEach(context -> lines.add(new TextLine(context, fonts.client(), "client")));
        }
        String popupName = name + "-details";
        ShowDetail showDetail = new ShowDetail(popupName, nodeType, null, null);
        TextBox tmpContent = new TextBox(lines, position, nodeType);
        int contentHeight = tmpContent.dimension().height() + 20;
        Position dbPosition = new Position(position.x(), position.y() + contentHeight);
        List<Store> stores = new ArrayList<>();
        for (AxonDB axonDB : hub.storage()) {
            Store newStore = new Store(showContexts ? new TextLine(axonDB.context(), fonts.messaging(), "axonhub"):null, dbPosition, new StyleClass("axondb" + (axonDB.master() ? " master" : "")));
            stores.add(newStore);
            dbPosition = new Position(dbPosition.x() + newStore.dimension().width() + 10, dbPosition.y());
        }
        int storesWidth = dbPosition.x() - position.x() - 10;
        if( storesWidth < tmpContent.dimension().width()) {
            stores = stores.stream().map(s -> s.move((tmpContent.dimension().width() - storesWidth)/2, 0)).collect(
                    Collectors.toList());
        }

        int x = position.x() + Math.max(tmpContent.dimension().width(), storesWidth)/2 - tmpContent.dimension().width()/2;
        TextBox content = new TextBox(lines, new Position(x, position.y()), nodeType);
        Element element = new Clickable(content, showDetail);
        boxMap.put(name, content);
        return new AxonHubGroup(element, stores, position);
    }

    @Override
    public Box get(String key) {
        return boxMap.get(key);
    }
}
