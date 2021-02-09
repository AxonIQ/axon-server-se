/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.logging.AuditLog;
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
import org.slf4j.Logger;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.security.Principal;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Controller that generates a SVG model of the configuration of AxonServer and the connected client applications.
 *
 * @author Marc Gathier
 */
@RestController("OverviewModel")
public class OverviewModel {

    private static final Logger auditLog = AuditLog.getLogger();

    private final Topology clusterController;
    private final Function<String, Stream<Application>> applicationProvider;
    private final Function<String, Stream<AxonServer>> axonServerProvider;
    private final Fonts fonts;

    public OverviewModel(Topology clusterController,
                         Function<String, Stream<Application>> applicationProvider,
                         Function<String, Stream<AxonServer>> axonServerProvider) {
        this.clusterController = clusterController;
        this.applicationProvider = applicationProvider;
        this.axonServerProvider = axonServerProvider;
        this.fonts = new Fonts();
    }

    @GetMapping("/v1/public/overview")
    public SvgOverview overview(final Principal principal,
                                @RequestParam(value = "for-context", required = false) String context) {
        auditLog.debug("[{}] Request to render an SVG cluster overview.", AuditLog.username(principal));

        boolean multiContext = clusterController.isMultiContext();
        AxonServerBoxMapping serverRegistry = new AxonServerBoxMapping(multiContext,
                                                                       clusterController.getName(),
                                                                       fonts);

        Element hubNodes = new Grouped(new Elements(10,
                                                    200,
                                                    () -> axonServerProvider.apply(context).iterator(),
                                                    serverRegistry), "axonserverNodes");
        Element clients = new Elements(10,
                                       10,
                                       () -> applicationProvider.apply(context).iterator(),
                                       new ApplicationBoxMapping(serverRegistry, fonts));
        Element popups = new Elements(() -> axonServerProvider.apply(context).iterator(),
                                      new AxonServerPopupMapping(serverRegistry, fonts));

        Elements components = new Elements(hubNodes, clients, popups);
        Element background = new Clickable(new Rectangle(new Position(0, 0),
                                                         components.dimension().increase(20, 20),
                                                         new StyleClass("background")), () -> "hidePopup()");

        return new SvgOverview(new Elements(background, components));
    }

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
