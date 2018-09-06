package io.axoniq.axonhub.rest;

import io.axoniq.axonhub.cluster.ClusterController;
import io.axoniq.axonhub.cluster.jpa.ClusterNode;
import io.axoniq.axonhub.rest.svg.mapping.Application;
import io.axoniq.axonhub.rest.svg.mapping.AxonHub;
import io.axoniq.axonhub.rest.svg.mapping.FakeApplication;
import io.axoniq.axonhub.rest.svg.mapping.FakeAxonHub;
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
        ClusterController clusterController = mock(ClusterController.class);
        Iterable<AxonHub> hubs = asList(new FakeAxonHub(true,
                                                        new ClusterNode("hub1","localhost","internal",1,2,3),
                                                        asSet("contex", "default"),
                                                        asSet( "default")),
                                        new FakeAxonHub(false,
                                                        new ClusterNode("hub2","localhost","internal",4,5,6),
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