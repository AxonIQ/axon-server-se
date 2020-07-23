package io.axoniq.axonserver.enterprise.replication.group;

import io.axoniq.axonserver.access.application.ReplicationGroupApplicationController;
import io.axoniq.axonserver.access.user.ReplicationGroupUserController;
import io.axoniq.axonserver.cluster.jpa.ReplicationGroupMember;
import io.axoniq.axonserver.cluster.jpa.ReplicationGroupMemberRepository;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.config.SystemInfoProvider;
import io.axoniq.axonserver.enterprise.ContextEvents;
import io.axoniq.axonserver.enterprise.component.processor.balancing.stategy.ReplicationGroupProcessorLoadBalancingRepository;
import io.axoniq.axonserver.enterprise.jpa.ReplicationGroupContext;
import io.axoniq.axonserver.enterprise.jpa.ReplicationGroupContextRepository;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.grpc.internal.Context;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupContexts;
import io.axoniq.axonserver.spring.FakeApplicationEventPublisher;
import org.junit.*;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class ReplicationGroupControllerTest {

    private ReplicationGroupController testSubject;
    private final FakeApplicationEventPublisher applicationEventPublisher = new FakeApplicationEventPublisher();
    private final Map<String, Map<String, ReplicationGroupContext>> replicationGroupContexts = new HashMap<>();

    @Before
    public void setUp() throws Exception {
        ReplicationGroupMemberRepository replicationGroupMemberRepository = mock(ReplicationGroupMemberRepository.class);

        when(replicationGroupMemberRepository.findByGroupIdAndNodeName(anyString(), anyString()))
                .thenReturn(Optional.of(new ReplicationGroupMember("Default", Node.newBuilder()
                                                                                  .setRole(Role.MESSAGING_ONLY)
                                                                                  .build())));

        ReplicationGroupContextRepository replicationGroupContextRepository = mock(ReplicationGroupContextRepository.class);
        when(replicationGroupContextRepository.save(any(ReplicationGroupContext.class)))
                .then(invocation -> {
                    ReplicationGroupContext instance = invocation.getArgument(0);
                    replicationGroupContexts.computeIfAbsent(instance.getReplicationGroupName(),
                                                             r -> new HashMap<>())
                                            .put(instance.getName(), instance);
                    return instance;
                });
        when(replicationGroupContextRepository.findByReplicationGroupName(anyString()))
                .then(invocation -> {
                    return new ArrayList<>(replicationGroupContexts.getOrDefault(invocation.getArgument(0),
                                                                                 Collections
                                                                                         .emptyMap())
                                                                   .values());
                });
        when(replicationGroupContextRepository.getOne(anyString())).then(invocation -> {
            String context = invocation.getArgument(0);
            return replicationGroupContexts.values()
                                           .stream()
                                           .filter(e -> e.containsKey(context))
                                           .map(e -> e.get(context))
                                           .findFirst().orElse(null);
        });
        when(replicationGroupContextRepository.findById(anyString())).then(invocation -> {
            String context = invocation.getArgument(0);
            return replicationGroupContexts.values()
                                           .stream()
                                           .filter(e -> e.containsKey(context))
                                           .map(e -> e.get(context))
                                           .findFirst();
        });
        doAnswer(invocation -> {
            ReplicationGroupContext context = invocation.getArgument(0);
            ReplicationGroupContext replicationGroupContext = replicationGroupContexts.values()
                                                                                      .stream()
                                                                                      .filter(e -> e
                                                                                              .containsKey(context.getName()))
                                                                                      .map(e -> e
                                                                                              .get(context.getName()))
                                                                                      .findFirst().orElse(null);
            if (replicationGroupContext != null) {
                replicationGroupContexts.get(replicationGroupContext.getReplicationGroupName()).remove(
                        replicationGroupContext.getName());
            }
            return null;
        }).when(replicationGroupContextRepository).delete(any(ReplicationGroupContext.class));

        doAnswer(invocation -> {
            String context = invocation.getArgument(0);
            ReplicationGroupContext replicationGroupContext = replicationGroupContexts.values()
                                                                                      .stream()
                                                                                      .filter(e -> e
                                                                                              .containsKey(context))
                                                                                      .map(e -> e.get(context))
                                                                                      .findFirst().orElse(null);
            if (replicationGroupContext != null) {
                replicationGroupContexts.get(replicationGroupContext.getReplicationGroupName()).remove(
                        replicationGroupContext.getName());
            }
            return null;
        }).when(replicationGroupContextRepository).deleteById(anyString());
        ReplicationGroupApplicationController applicationController = mock(ReplicationGroupApplicationController.class);

        ReplicationGroupUserController userController = mock(ReplicationGroupUserController.class);
        ReplicationGroupProcessorLoadBalancingRepository processorLoadBalancingRepository = mock(
                ReplicationGroupProcessorLoadBalancingRepository.class);

        MessagingPlatformConfiguration messagingPlatformConfiguration = new MessagingPlatformConfiguration(new SystemInfoProvider() {
            @Override
            public String getHostName() throws UnknownHostException {
                return "hostname";
            }
        });

        replicationGroupContexts.computeIfAbsent("Default", d -> new HashMap<>())
                                .put("Context1", new ReplicationGroupContext("Context1", "Default"));
        replicationGroupContexts.computeIfAbsent("Default", d -> new HashMap<>())
                                .put("Context2", new ReplicationGroupContext("Context2", "Default"));
        testSubject = new ReplicationGroupController(replicationGroupMemberRepository,
                                                     replicationGroupContextRepository,
                                                     applicationEventPublisher,
                                                     applicationController,
                                                     userController,
                                                     processorLoadBalancingRepository,
                                                     messagingPlatformConfiguration);
    }

    @Test
    public void addContext() {
        testSubject.addContext("Group1", "Context1", Collections.singletonMap("key", "value"));
        assertNotNull(replicationGroupContexts.get("Group1"));
        assertNotNull(replicationGroupContexts.get("Group1").get("Context1"));
    }

    @Test
    public void findByReplicationGroupName() {
        List<ReplicationGroupContext> contexts = testSubject
                .findContextsByReplicationGroupName("Default");
        assertEquals(2, contexts.size());
        contexts.sort(Comparator.comparing(ReplicationGroupContext::getName));
        assertEquals("Context1", contexts.get(0).getName());
        assertEquals("Context2", contexts.get(1).getName());
    }

    @Test
    public void merge() {
        testSubject.merge(ReplicationGroupContexts.newBuilder()
                                                  .setReplicationGroupName("Default")
                                                  .addContext(Context.newBuilder()
                                                                     .setContextName("Context1")
                                                                     .build())
                                                  .addContext(Context.newBuilder()
                                                                     .setContextName("Context3")
                                                                     .build())
                                                  .build(), Role.PRIMARY);

        assertNotNull(replicationGroupContexts.get("Default").get("Context1"));
        assertNotNull(replicationGroupContexts.get("Default").get("Context3"));
        assertNull(replicationGroupContexts.get("Default").get("Context2"));

        Set<String> deletedContexts = new HashSet<>();
        Set<String> createdContexts = new HashSet<>();
        applicationEventPublisher.events().forEach(e -> {
            if (e instanceof ContextEvents.ContextCreated) {
                createdContexts.add(((ContextEvents.ContextCreated) e).context());
            }
            if (e instanceof ContextEvents.ContextDeleted) {
                deletedContexts.add(((ContextEvents.ContextDeleted) e).context());
            }
        });

        assertTrue(deletedContexts.contains("Context2"));
        assertTrue(createdContexts.contains("Context3"));
    }

    @Test
    public void mergeMessagingOnly() {
        testSubject.merge(ReplicationGroupContexts.newBuilder()
                                                  .setReplicationGroupName("Default")
                                                  .addContext(Context.newBuilder()
                                                                     .setContextName("Context1")
                                                                     .build())
                                                  .addContext(Context.newBuilder()
                                                                     .setContextName("Context2")
                                                                     .build())
                                                  .addContext(Context.newBuilder()
                                                                     .setContextName("Context3")
                                                                     .build())
                                                  .build(), Role.MESSAGING_ONLY);
        applicationEventPublisher.events().forEach(e -> {
            if (e instanceof ContextEvents.ContextCreated) {
                assertEquals(Role.MESSAGING_ONLY, ((ContextEvents.ContextCreated) e).role());
            }
        });
    }

    @Test
    public void deleteContext() {
        testSubject.deleteContext("Context1", false);
        assertEquals(1, replicationGroupContexts.get("Default").size());
    }

    @Test
    public void deleteContextNotFound() {
        testSubject.deleteContext("Context12", false);
        assertEquals(2, replicationGroupContexts.get("Default").size());
    }

    @Test
    public void deleteReplicationGroup() {
        testSubject.deleteReplicationGroup("Default", true);
        assertEquals(0, replicationGroupContexts.get("Default").size());
    }

    @Test
    public void getContextNames() {
        List<String> result = testSubject.getContextNames("Default");
        assertTrue(result.contains("Context1"));
        assertTrue(result.contains("Context2"));
    }

    @Test
    public void getMyRole() {
        assertEquals(Role.MESSAGING_ONLY, testSubject.getMyRole("Group1"));
    }
}