package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.KeepNames;
import io.axoniq.axonserver.rest.svg.Element;
import io.axoniq.axonserver.rest.svg.Elements;
import io.axoniq.axonserver.rest.svg.Fonts;
import io.axoniq.axonserver.rest.svg.attribute.Position;
import io.axoniq.axonserver.rest.svg.attribute.StyleClass;
import io.axoniq.axonserver.rest.svg.decorator.Clickable;
import io.axoniq.axonserver.rest.svg.decorator.Grouped;
import io.axoniq.axonserver.rest.svg.element.Rectangle;
import io.axoniq.axonserver.rest.svg.mapping.Application;
import io.axoniq.axonserver.rest.svg.mapping.ApplicationBoxMapping;
import io.axoniq.axonserver.rest.svg.mapping.AxonServer;
import io.axoniq.axonserver.rest.svg.mapping.AxonServerBoxMapping;
import io.axoniq.axonserver.rest.svg.mapping.AxonServerPopupMapping;
import io.axoniq.axonserver.topology.Topology;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * @author Marc Gathier
 */
@RestController("OverviewModel")
public class OverviewModel {

    private final Topology clusterController;
    private final Iterable<Application> applications;
    private final Iterable<AxonServer> axonServers;
    private final Fonts fonts;

    public OverviewModel(Topology clusterController,
                         Iterable<Application> applications,
                         Iterable<AxonServer> axonServers) {
        this.clusterController = clusterController;
        this.applications = applications;
        this.axonServers = axonServers;
        this.fonts = new Fonts();
    }

    @RequestMapping("/v1/public/overview")
    public SvgOverview overview() {
        boolean multiContext = clusterController.isMultiContext();
        AxonServerBoxMapping hubRegistry = new AxonServerBoxMapping(multiContext, clusterController.getName(), fonts);

        Element hubNodes = new Grouped(new Elements(10, 200, axonServers, hubRegistry), "axonserverNodes");
        Element clients = new Elements(10, 10, applications, new ApplicationBoxMapping(hubRegistry, fonts));
        Element popups = new Elements(axonServers, new AxonServerPopupMapping(hubRegistry, fonts));

        Elements components = new Elements(hubNodes, clients, popups);
        Element background = new Clickable(new Rectangle(new Position(0, 0),
                                            components.dimension().increase(20, 20),
                                            new StyleClass("background")), ()-> "hidePopup()");

        return new SvgOverview(new Elements(background, components));
    }

    @KeepNames
    public static class SvgOverview {
        private final Element element;

        SvgOverview(Element element) {
            this.element = element;
        }

        public String getSvgObjects() {
            StringWriter stringWriter = new StringWriter();
            element.printOn(new PrintWriter(stringWriter));
            return stringWriter.toString();
        }
        public int getWidth() {
            return element.dimension().width();
        }
        public int getHeight() {
            return element.dimension().height();
        }
    }
}
