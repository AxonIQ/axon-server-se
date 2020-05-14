package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.RaftNode;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.config.SystemInfoProvider;
import io.axoniq.axonserver.enterprise.config.RaftProperties;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import org.junit.*;
import org.springframework.context.ApplicationEventPublisher;

import java.net.UnknownHostException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class GrpcRaftControllerTest {

    private GrpcRaftController testSubject;
    private SystemInfoProvider systemInfoProvider = new SystemInfoProvider() {
        @Override
        public String getHostName() throws UnknownHostException {
            return "me";
        }
    };
    RaftProperties raftProperties = new RaftProperties(systemInfoProvider);

    @Before
    public void init() {

        MessagingPlatformConfiguration messagingPlatformConfiguration = new MessagingPlatformConfiguration(
                systemInfoProvider);


        RaftGroupRepositoryManager raftGroupNodeRepository = mock(RaftGroupRepositoryManager.class);

        ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);

        GrpcRaftGroupFactory groupFactory = mock(GrpcRaftGroupFactory.class);
        when(groupFactory.create(anyString(), anyString())).thenAnswer(invocation -> {
            RaftNode raftNode = mock(RaftNode.class);
            when(raftNode.groupId()).thenReturn(invocation.getArgument(0));
            when(raftNode.nodeId()).thenReturn(invocation.getArgument(1));
            RaftGroup raftGroup = mock(RaftGroup.class);
            when(raftGroup.localNode()).thenReturn(raftNode);
            return raftGroup;
        });
        testSubject = new GrpcRaftController(messagingPlatformConfiguration,
                                             raftProperties,
                                             raftGroupNodeRepository,
                                             eventPublisher,
                                             groupFactory);
    }

    @Test
    public void delete() throws InterruptedException {
        raftProperties.setMinElectionTimeout(100);
        raftProperties.setMaxElectionTimeout(200);
        testSubject.delete("demo", false);
        try {
            testSubject.getOrCreateRaftNode("demo", "myNodeId");
            fail("Should fail immediately after delete");
        } catch (MessagingPlatformException mpe) {
            assertEquals(ErrorCode.CONTEXT_NOT_FOUND, mpe.getErrorCode());
        }
        Thread.sleep(raftProperties.getMaxElectionTimeout() * 2);
        RaftNode raftNode = testSubject.getOrCreateRaftNode("demo", "myNodeId");
        assertNotNull(raftNode);
    }
}