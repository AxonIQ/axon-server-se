/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.rest.svg.mapping.Application;
import io.axoniq.axonserver.rest.svg.mapping.AxonServer;
import io.axoniq.axonserver.rest.svg.mapping.FakeApplication;
import io.axoniq.axonserver.rest.svg.mapping.FakeAxonServer;
import io.axoniq.axonserver.topology.SimpleAxonServerNode;
import io.axoniq.axonserver.topology.Topology;
import org.junit.*;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.hibernate.validator.internal.util.CollectionHelper.asSet;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class OverviewModelTest {

    private OverviewModel testSubject;
    private AxonServersOverviewProvider axonServersOverviewProvider;


    @Before
    public void setUp() {
        Topology clusterController = mock(Topology.class);
        List<AxonServer> hubs = asList(new FakeAxonServer(true,
                                                          new SimpleAxonServerNode("hub1", "localhost", 1, 2),
                                                          asSet("contex", "default"),
                                                          asSet("default")),
                                       new FakeAxonServer(false,
                                                          new SimpleAxonServerNode("hub2", "localhost", 4, 5),
                                                          asSet("contex"),
                                                          asSet()));

        List<Application> applications = singletonList(new FakeApplication("app",
                                                                           "comp",
                                                                           "context",
                                                                           2,
                                                                           asList("hub1", "hub2")));

        axonServersOverviewProvider = new AxonServersOverviewProvider(context -> applications.stream(),
                                                                      context -> hubs.stream());

        testSubject = new OverviewModel(clusterController, c -> applications.stream(), c -> hubs.stream());
    }

    @Test
    public void overview() {
        OverviewModel.SvgOverview overview = testSubject.overview(null, null);
        assertTrue(overview.getHeight() > 0);
        assertTrue(overview.getWidth() > 0);
        assertTrue(overview.getSvgObjects().length() > 0);
        System.out.println(overview.getSvgObjects());
    }

    @Test
    public void overviewV2() {
        AxonServersOverviewProvider.ApplicationsAndNodes applicationsAndNodes = axonServersOverviewProvider
                .applicationsAndNodes(null);
        assertEquals("app", applicationsAndNodes.getApplications().get(0).getName());
        assertEquals("context", applicationsAndNodes.getApplications().get(0).getContext());
        assertEquals(2, applicationsAndNodes.getApplications().get(0).getInstances());

        assertEquals("hub1", applicationsAndNodes.getNodes().get(0).getName());
        assertEquals("hub2", applicationsAndNodes.getNodes().get(1).getName());
        assertEquals("localhost", applicationsAndNodes.getNodes().get(0).getHostName());
        assertEquals(Integer.valueOf(2), applicationsAndNodes.getNodes().get(0).getHttpPort());
        assertEquals(Integer.valueOf(5), applicationsAndNodes.getNodes().get(1).getHttpPort());
    }

}
