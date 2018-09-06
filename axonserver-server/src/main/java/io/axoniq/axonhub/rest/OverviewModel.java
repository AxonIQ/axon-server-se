package io.axoniq.axonhub.rest;

import io.axoniq.axonhub.KeepNames;
import io.axoniq.axonhub.cluster.ClusterController;
import io.axoniq.axonhub.rest.svg.Element;
import io.axoniq.axonhub.rest.svg.Elements;
import io.axoniq.axonhub.rest.svg.Fonts;
import io.axoniq.axonhub.rest.svg.attribute.Position;
import io.axoniq.axonhub.rest.svg.attribute.StyleClass;
import io.axoniq.axonhub.rest.svg.decorator.Clickable;
import io.axoniq.axonhub.rest.svg.decorator.Grouped;
import io.axoniq.axonhub.rest.svg.element.Rectangle;
import io.axoniq.axonhub.rest.svg.mapping.Application;
import io.axoniq.axonhub.rest.svg.mapping.ApplicationBoxMapping;
import io.axoniq.axonhub.rest.svg.mapping.AxonHub;
import io.axoniq.axonhub.rest.svg.mapping.AxonHubBoxMapping;
import io.axoniq.axonhub.rest.svg.mapping.AxonHubPopupMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Author: marc
 */
@RestController("OverviewModel")
public class OverviewModel {

    private final ClusterController clusterController;
    private final Iterable<Application> applications;
    private final Iterable<AxonHub> axonHubs;
    private final Fonts fonts;

    public OverviewModel(ClusterController clusterController,
                         Iterable<Application> applications,
                         Iterable<AxonHub> axonHubs) {
        this.clusterController = clusterController;
        this.applications = applications;
        this.axonHubs = axonHubs;
        this.fonts = new Fonts();
    }

    @RequestMapping("/v1/public/overview")
    public SvgOverview overview() {
        boolean multiContext = clusterController.isMultiContext();
        AxonHubBoxMapping hubRegistry = new AxonHubBoxMapping(multiContext, clusterController.getName(), fonts);

        Element hubNodes = new Grouped(new Elements(10, 200, axonHubs, hubRegistry), "axonhubNodes");
        Element clients = new Elements(10, 10, applications, new ApplicationBoxMapping(hubRegistry, fonts));
        Element popups = new Elements(axonHubs, new AxonHubPopupMapping(hubRegistry, fonts));

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
