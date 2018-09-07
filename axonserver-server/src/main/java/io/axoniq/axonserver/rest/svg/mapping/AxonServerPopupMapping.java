package io.axoniq.axonserver.rest.svg.mapping;

import io.axoniq.axonserver.cluster.jpa.ClusterNode;
import io.axoniq.axonserver.rest.svg.BoxRegistry;
import io.axoniq.axonserver.rest.svg.Fonts;
import io.axoniq.axonserver.rest.svg.Mapping;
import io.axoniq.axonserver.rest.svg.TextLine;
import io.axoniq.axonserver.rest.svg.attribute.Position;
import io.axoniq.axonserver.rest.svg.decorator.Hidden;
import io.axoniq.axonserver.rest.svg.element.Rectangle;
import io.axoniq.axonserver.rest.svg.element.TextBox;

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
        ClusterNode node = hub.node();
        List<String> lines = new ArrayList<>();
        lines.add("Hostname: " + node.getHostName());
        lines.add("Grpc Port: " + node.getGrpcPort());
        lines.add("Internal Hostname: " + node.getInternalHostName());
        lines.add("Internal Grpc Port: " + node.getGrpcInternalPort());
        lines.add("Http Port: " + node.getHttpPort());
        List<TextLine> textLines = lines.stream().map(text -> new TextLine(text, fonts.popup(), "popup")).collect(toList());
        Rectangle r = hubRegistry.get(node.getName()).rectangle();
        Position position = r.position().shift(10, r.height()-5);
        TextBox content = new TextBox(textLines, position, "popup", new TextBox.Left(5), 10);
        return new Hidden(node.getName() + "-details", content, "popup");
    }
}
