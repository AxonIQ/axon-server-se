package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.rest.svg.mapping.Application;
import io.axoniq.axonserver.rest.svg.mapping.AxonServer;
import io.axoniq.axonserver.rest.svg.mapping.FakeApplication;
import io.axoniq.axonserver.rest.svg.mapping.FakeAxonServer;
import io.axoniq.axonserver.topology.SimpleAxonServerNode;
import io.axoniq.axonserver.topology.Topology;
import org.junit.*;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static junit.framework.TestCase.assertTrue;
import static org.hibernate.validator.internal.util.CollectionHelper.asSet;
import static org.mockito.Mockito.*;

/**
 * Author: marc
 */
public class OverviewModelTest {

    private OverviewModel testSubject;

    @Before
    public void setUp() {
        Topology clusterController = mock(Topology.class);
        Iterable<AxonServer> hubs = asList(new FakeAxonServer(true,
                                                              new SimpleAxonServerNode("hub1","localhost",1,2),
                                                              asSet("contex", "default"),
                                                              asSet( "default")),
                                           new FakeAxonServer(false,
                                                              new SimpleAxonServerNode("hub2", "localhost",  4, 5),
                                                              asSet("contex"),
                                                              asSet() ));

        Iterable<Application> applications = singletonList(new FakeApplication("app",
                                                                               "comp",
                                                                               "context",
                                                                               2,
                                                                               asList("hub1", "hub2")));

        testSubject = new OverviewModel(clusterController, applications, hubs);
    }

    @Test
    public void overview() {
        OverviewModel.SvgOverview overview = testSubject.overview();
        assertTrue(overview.getHeight() > 0);
        assertTrue(overview.getWidth() > 0);
        assertTrue(overview.getSvgObjects().length() > 0);
        System.out.println(overview.getSvgObjects());
    }
}