package io.axoniq.axonserver.enterprise.context;

import io.axoniq.axonserver.AxonServerEnterprise;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.cluster.internal.RemoteConnection;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.enterprise.jpa.Context;
import io.axoniq.axonserver.features.FeatureChecker;
import io.axoniq.axonserver.grpc.internal.ContextRole;
import io.axoniq.axonserver.topology.Topology;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.persistence.EntityManager;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Author: marc
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = AxonServerEnterprise.class)
@EnableAutoConfiguration
@EntityScan("io.axoniq")
@DataJpaTest
public class ContextControllerTest {

    private ContextController testSubject;

    @Autowired
    private EntityManager entityManager;

    @Mock
    private ApplicationEventPublisher eventPublisher;

    @Mock
    private ClusterController clusterController;


    @Before
    public void setUp()  {
        Context defaultContext = new Context(Topology.DEFAULT_CONTEXT);
        FeatureChecker limits = new FeatureChecker() {
            @Override
            public boolean isEnterprise() {
                return true;
            }

            @Override
            public int getMaxContexts() {
                return 3;
            }
        };
        ClusterNode node1 = new ClusterNode("node1", null, null, null, null, null);
        ClusterNode node2 = new ClusterNode("node2", null, null, null, null, null);
        node1.addContext(defaultContext, true, true);
        node2.addContext(defaultContext, true, true);
        entityManager.persist(node1);
        entityManager.persist(node2);
        entityManager.flush();
        testSubject = new ContextController(entityManager, clusterController, eventPublisher);
    }

    @Test
    public void getContexts() {
        List<Context> contexts = testSubject.getContexts().collect(Collectors
                                                                           .toList());
        assertEquals(1, contexts.size());
    }

    @Test
    public void addNodeToContext() {
        ClusterNode node3 = new ClusterNode("node3", null, null, null, null, null);
        entityManager.persist(node3);
        testSubject.updateNodeRoles(Topology.DEFAULT_CONTEXT, "node3", true, true, false);
        Context defaultContext = entityManager.find(Context.class, Topology.DEFAULT_CONTEXT);
        assertEquals(3, defaultContext.getStorageNodes().size());
    }

    @Test
    public void deleteContext() {
        // given context test1 connected to nodes 1 and 2
        Context test1 = new Context("test1");
        entityManager.persist(test1);

        entityManager.createQuery("select c from ClusterNode c", ClusterNode.class).getResultList().forEach(n -> n.addContext(test1, true, true));
        // when delete context test1
        testSubject.deleteContext("test1", false);
        // expect nodes 1 and node 2 no longer contain context text1
        entityManager.createQuery("select c from ClusterNode c", ClusterNode.class).getResultList().forEach(n -> assertFalse(n.getContextNames().contains("test1")));
    }

    @Test
    public void deleteNodeFromContext() {
        testSubject.deleteNodeFromContext(Topology.DEFAULT_CONTEXT, "node1", false);
        ClusterNode node1 = entityManager.find(ClusterNode.class, "node1");
        assertEquals(0, node1.getContextNames().size());
    }

    @Test
    public void addContext() {
        testSubject.addContext("test1", Collections.singletonList(new NodeRoles("node1", false, false)), false);

        ClusterNode node1 = entityManager.find(ClusterNode.class, "node1");
        assertEquals(2, node1.getContextNames().size());
    }

    @Test
    public void update() {
    }

    @Test
    public void on() {
        RemoteConnection remoteConnection = mock(RemoteConnection.class);
        ClusterNode node2 = new ClusterNode("node2", null, null, null, null, null);
        when(remoteConnection.getClusterNode()).thenReturn(node2);

        ClusterEvents.AxonServerInstanceConnected axonhubInstanceConnected = new ClusterEvents.AxonServerInstanceConnected(remoteConnection, Collections.emptyList(),
                                                                                                                           Collections
                                                                                                                             .singletonList(
                                                                                                                                     ContextRole.newBuilder().setName("test1").build()), Collections.emptyList());
        testSubject.on(axonhubInstanceConnected);
    }
}