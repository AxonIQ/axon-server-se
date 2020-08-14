package io.axoniq.axonserver.enterprise.replication;

import io.axoniq.axonserver.cluster.jpa.ReplicationGroupMember;
import io.axoniq.axonserver.cluster.jpa.ReplicationGroupMemberRepository;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.jpa.AdminReplicationGroupRepository;
import io.axoniq.axonserver.enterprise.jpa.ReplicationGroupContext;
import io.axoniq.axonserver.enterprise.jpa.ReplicationGroupContextRepository;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.util.ContextNotFoundException;
import org.junit.*;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class RaftGroupRepositoryManagerTest {
    private RaftGroupRepositoryManager testSubject;
    private MessagingPlatformConfiguration messagingPlatformConfiguration;

    @Before
    public void init() {
        ReplicationGroupMemberRepository raftGroupNodeRepository = mock(ReplicationGroupMemberRepository.class);

        Set<ReplicationGroupMember> nodes = new HashSet<>();
        nodes.add(new ReplicationGroupMember("context", node("id1", "primary", Role.PRIMARY)));
        nodes.add(new ReplicationGroupMember("context", node("id2", "secondary", Role.SECONDARY)));
        nodes.add(new ReplicationGroupMember("context", node("id3", "passive_backup", Role.PASSIVE_BACKUP)));
        nodes.add(new ReplicationGroupMember("context", node("id4", "messaging", Role.MESSAGING_ONLY)));
        nodes.add(new ReplicationGroupMember("context2", node("id5", "primary", Role.PRIMARY)));

        when(raftGroupNodeRepository.findByGroupId(anyString())).then(invocation -> {
            String context = invocation.getArgument(0);
            return nodes.stream().filter(n -> n.getGroupId().equals(context)).collect(Collectors.toSet());
        });
        when(raftGroupNodeRepository.findByGroupIdAndNodeName(anyString(), anyString())).then(invocation -> {
            String context = invocation.getArgument(0);
            String node = invocation.getArgument(1);
            return nodes.stream()
                        .filter(n -> n.getGroupId().equals(context))
                        .filter(n -> n.getNodeName().equals(node))
                        .findFirst();
        });
        when(raftGroupNodeRepository.findByNodeName(anyString())).then(invocation -> {
            String node = invocation.getArgument(0);
            return nodes.stream()
                        .filter(n -> n.getNodeName().equals(node))
                        .collect(Collectors.toSet());
        });

        messagingPlatformConfiguration = mock(MessagingPlatformConfiguration.class);
        AdminReplicationGroupRepository adminReplicationGroupRepository = mock(AdminReplicationGroupRepository.class);

        ReplicationGroupContextRepository replicationGroupContextRepository = mock(ReplicationGroupContextRepository.class);
        when(replicationGroupContextRepository.findByReplicationGroupName(anyString()))
                .then(invocation -> Collections.singletonList(new ReplicationGroupContext(invocation.getArgument(0),
                                                                                          invocation.getArgument(0))));
        testSubject = new RaftGroupRepositoryManager(raftGroupNodeRepository,
                                                     replicationGroupContextRepository,
                                                     adminReplicationGroupRepository,
                                                     messagingPlatformConfiguration);
    }

    private Node node(String id, String name, Role role) {
        return Node.newBuilder().setRole(role).setNodeName(name).setNodeId(id).build();
    }

    @Test
    public void isMultiTierPrimary() {
        when(messagingPlatformConfiguration.getName()).thenReturn("primary");
        assertTrue(testSubject.hasLowerTier("context"));
        assertFalse(testSubject.hasLowerTier("context2"));
    }

    @Test
    public void isMultiTierSecondary() {
        when(messagingPlatformConfiguration.getName()).thenReturn("secondary");
        assertFalse(testSubject.hasLowerTier("context"));
    }

    @Test
    public void isMultiTierMessaging() {
        when(messagingPlatformConfiguration.getName()).thenReturn("messaging");
        assertTrue(testSubject.hasLowerTier("context"));
    }

    @Test
    public void isMultiTierBackup() {
        when(messagingPlatformConfiguration.getName()).thenReturn("passive_backup");
        assertFalse(testSubject.hasLowerTier("context"));
    }

    @Test
    public void nextTierEventStoresForPrimary() {
        when(messagingPlatformConfiguration.getName()).thenReturn("primary");
        assertEquals(Collections.singleton("secondary"), testSubject.nextTierEventStores("context"));
        assertEquals(Collections.emptySet(), testSubject.nextTierEventStores("context2"));
    }

    @Test
    public void nextTierEventStoresForMessaging() {
        when(messagingPlatformConfiguration.getName()).thenReturn("messaging");
        assertEquals(Collections.singleton("secondary"), testSubject.nextTierEventStores("context"));
    }

    @Test
    public void nextTierEventStoresForSecondary() {
        when(messagingPlatformConfiguration.getName()).thenReturn("secondary");
        assertEquals(Collections.emptySet(), testSubject.nextTierEventStores("context"));
    }

    @Test
    public void nextTierEventStoresForBackup() {
        when(messagingPlatformConfiguration.getName()).thenReturn("passive_backup");
        assertEquals(Collections.emptySet(), testSubject.nextTierEventStores("context"));
    }

    @Test
    public void storageContexts() {
        when(messagingPlatformConfiguration.getName()).thenReturn("primary");
        Set<String> storageContexts = testSubject.storageContexts();
        assertEquals(2, storageContexts.size());
        assertTrue((storageContexts.contains("context")));
        assertTrue((storageContexts.contains("context2")));
    }

    @Test
    public void storageContextsFromMessagingOnlyNode() {
        when(messagingPlatformConfiguration.getName()).thenReturn("messaging");
        Set<String> storageContexts = testSubject.storageContexts();
        assertEquals(0, storageContexts.size());
    }

    @Test
    public void hasLowerTier() {
        when(messagingPlatformConfiguration.getName()).thenReturn("primary");
        assertTrue(testSubject.hasLowerTier("context"));
        assertFalse(testSubject.hasLowerTier("context2"));
    }

    @Test(expected = ContextNotFoundException.class)
    public void hasLowerTierContextNotOnCurrentNode() {
        when(messagingPlatformConfiguration.getName()).thenReturn("messaging");
        testSubject.hasLowerTier("context2");
    }

    @Test(expected = ContextNotFoundException.class)
    public void hasLowerTierFailsForUnknownContext() {
        when(messagingPlatformConfiguration.getName()).thenReturn("messaging");
        testSubject.hasLowerTier("context3");
    }
}