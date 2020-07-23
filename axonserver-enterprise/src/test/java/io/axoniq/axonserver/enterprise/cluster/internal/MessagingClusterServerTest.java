package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.TestSystemInfoProvider;
import io.axoniq.axonserver.cluster.grpc.LeaderElectionService;
import io.axoniq.axonserver.cluster.grpc.LogReplicationService;
import io.axoniq.axonserver.config.FeatureChecker;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.config.SslConfiguration;
import io.axoniq.axonserver.enterprise.replication.admin.GrpcRaftConfigService;
import io.axoniq.axonserver.enterprise.replication.group.GrpcRaftGroupService;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.junit.*;
import org.springframework.context.ApplicationEventPublisher;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
@RunWith(MockitoJUnitRunner.class)
public class MessagingClusterServerTest {
    private MessagingClusterServer testSubject;
    @Mock
    private MessagingClusterService clusterService;
    private MessagingPlatformConfiguration configuration;

    @Mock
    private InternalEventStoreService internalEventStore;
    private FeatureChecker limits;
    @Mock
    private LogReplicationService logReplicationService;
    @Mock
    private LeaderElectionService leaderElectionService;
    @Mock
    private GrpcRaftGroupService grpcRaftGroupService;
    @Mock
    private GrpcRaftConfigService grpcRaftConfigService;
    @Mock
    private ApplicationEventPublisher applicationEventPublisher;

    @Before
    public void setUp() {
        configuration = new MessagingPlatformConfiguration(new TestSystemInfoProvider());
        configuration.setSsl(new SslConfiguration());
        configuration.setInternalPort(18224);
        limits = new FeatureChecker() {
            @Override
            public boolean isEnterprise() {
                return true;
            }
        };

        testSubject = new MessagingClusterServer(configuration,
                                                 logReplicationService,
                                                 leaderElectionService,
                                                 Arrays.asList(
                                                         clusterService, internalEventStore,
                                                         grpcRaftGroupService,
                                                         grpcRaftConfigService
                                                 ),
                                                 limits, mock(ApplicationEventPublisher.class));
    }

    @Test(expected = RuntimeException.class)
    public void startWithoutCertificateFile() {
        configuration.getSsl().setEnabled(true);
        testSubject.start();
    }

    @Test
    public void startWithoutSsl() {
        assertTrue(testSubject.isAutoStartup());
        testSubject.start();
        assertTrue(testSubject.isRunning());
        AtomicBoolean callbackExecuted = new AtomicBoolean(false);
        testSubject.stop(() -> {callbackExecuted.set(true);});
        assertTrue(callbackExecuted.get());
    }

    @Test
    public void startWithSsl() throws Exception {
        configuration.getSsl().setEnabled(true);
        configuration.getSsl().setCertChainFile("../resources/axoniq-public.crt");
        configuration.getSsl().setPrivateKeyFile("../resources/axoniq-private.pem");
        assertTrue(testSubject.isAutoStartup());
        testSubject.start();
        assertTrue(testSubject.isRunning());
        testSubject.stop();
        Thread.sleep(50);
    }

}
