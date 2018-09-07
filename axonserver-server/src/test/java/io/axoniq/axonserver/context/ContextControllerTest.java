package io.axoniq.axonserver.context;

import io.axoniq.axonserver.ClusterEvents;
import io.axoniq.axonserver.cluster.jpa.ClusterNode;
import io.axoniq.axonserver.context.jpa.Context;
import io.axoniq.axonserver.grpc.internal.RemoteConnection;
import io.axoniq.axonhub.internal.grpc.ContextRole;
import io.axoniq.axonserver.licensing.Limits;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
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
@DataJpaTest
public class ContextControllerTest {

    private ContextController testSubject;

    @Autowired
    private EntityManager entityManager;

    @Mock
    private Limits limits;

    @Mock
    private ApplicationEventPublisher eventPublisher;


    @Before
    public void setUp()  {
        Context defaultContext = new Context(ContextController.DEFAULT);
        ClusterNode node1 = new ClusterNode("node1", null, null, null, null, null);
        ClusterNode node2 = new ClusterNode("node2", null, null, null, null, null);
        node1.addContext(defaultContext, true, true);
        node2.addContext(defaultContext, true, true);
        entityManager.persist(node1);
        entityManager.persist(node2);
        entityManager.flush();
        testSubject = new ContextController(entityManager, eventPublisher);
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
        testSubject.addNodeToContext(ContextController.DEFAULT, "node3", true, true, false);
        Context defaultContext = entityManager.find(Context.class, ContextController.DEFAULT);
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
        testSubject.deleteNodeFromContext(ContextController.DEFAULT, "node1", false);
        ClusterNode node1 = entityManager.find(ClusterNode.class, "node1");
        assertEquals(0, node1.getContextNames().size());
    }

    @Test
    public void addContext() {
        when(limits.isAddContextAllowed()).thenReturn(true);
        when(limits.getMaxContexts()).thenReturn(3);
        testSubject.addContext("test1", Collections.singletonList(new NodeRoles("node1", false, false)), false);

        ClusterNode node1 = entityManager.find(ClusterNode.class, "node1");
        assertEquals(2, node1.getContextNames().size());
    }

    @Test
    public void update() {
    }

    @Test
    public void on() {
        when(limits.isAddContextAllowed()).thenReturn(true);
        when(limits.getMaxContexts()).thenReturn(3);
        RemoteConnection remoteConnection = mock(RemoteConnection.class);
        ClusterNode node2 = new ClusterNode("node2", null, null, null, null, null);
        when(remoteConnection.getClusterNode()).thenReturn(node2);

        ClusterEvents.AxonHubInstanceConnected axonhubInstanceConnected = new ClusterEvents.AxonHubInstanceConnected(remoteConnection, 0,
                                                                                                                     Collections
                                                                                                                             .singletonList(
                                                                                                                                     ContextRole.newBuilder().setName("test1").build()), Collections.emptyList());
        testSubject.on(axonhubInstanceConnected);
    }
}