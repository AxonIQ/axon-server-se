/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.config.FeatureChecker;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.version.DefaultVersionInfoProvider;
import io.axoniq.axonserver.config.SystemInfoProvider;
import io.axoniq.axonserver.message.command.CommandDispatcher;
import io.axoniq.axonserver.message.event.EventDispatcher;
import io.axoniq.axonserver.message.query.QueryDispatcher;
import io.axoniq.axonserver.message.query.subscription.FakeSubscriptionMetrics;
import io.axoniq.axonserver.rest.json.NodeConfiguration;
import io.axoniq.axonserver.rest.svg.mapping.AxonServers;
import io.axoniq.axonserver.topology.DefaultEventStoreLocator;
import io.axoniq.axonserver.topology.DefaultTopology;
import io.axoniq.axonserver.topology.Topology;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.runners.*;

import java.net.UnknownHostException;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
@RunWith(MockitoJUnitRunner.class)
public class PublicRestControllerTest {
    private PublicRestController testSubject;
    private FeatureChecker limits = new FeatureChecker() {
    };
    private Topology clusterController;
    @Mock
    private CommandDispatcher commandDispatcher;
    @Mock
    private QueryDispatcher queryDispatcher;
    @Mock
    private EventDispatcher eventDispatcher;

    @Before
    public void setup() {
        MessagingPlatformConfiguration messagePlatformConfiguration = new MessagingPlatformConfiguration(new SystemInfoProvider() {
            @Override
            public int getPort() {
                return 8080;
            }

            @Override
            public String getHostName() throws UnknownHostException {
                return "DEMO";
            }
        });
        clusterController = new DefaultTopology(messagePlatformConfiguration);
        testSubject = new PublicRestController(new AxonServers(clusterController),
                                               clusterController,
                                               commandDispatcher,
                                               queryDispatcher,
                                               eventDispatcher,
                                               limits,
                                               messagePlatformConfiguration,
                                               new DefaultVersionInfoProvider(),
                                               () -> new FakeSubscriptionMetrics(500, 400, 1000));

    }

    @Test
    public void getClusterNodes() {
        List<PublicRestController.JsonServerNode> nodes = testSubject.getClusterNodes();
        assertEquals(1, nodes.size());
        assertEquals("DEMO", nodes.get(0).getName());
    }

    @Test
    public void getNodeInfo() {
        NodeConfiguration node = testSubject.getNodeConfiguration();
        assertEquals("DEMO", node.getName());
        assertEquals("DEMO", node.getHostName());
        assertNull( node.getInternalHostName());
        assertEquals(Integer.valueOf(8124), node.getGrpcPort());
        assertEquals(Integer.valueOf(8080), node.getHttpPort());
    }


    @Test
    public void licenseInfo() {
        LicenseInfo licenseInfo = testSubject.licenseInfo();
        assertEquals("Standard edition", licenseInfo.getEdition());
    }

}
