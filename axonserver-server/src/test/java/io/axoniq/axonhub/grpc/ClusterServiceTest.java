package io.axoniq.axonhub.grpc;

import io.axoniq.axonhub.cluster.ClusterController;
import io.axoniq.axonhub.cluster.jpa.ClusterNode;
import io.axoniq.axonhub.config.MessagingPlatformConfiguration;
import io.axoniq.axonhub.context.ContextController;
import io.axoniq.axonhub.licensing.Limits;
import io.axoniq.platform.grpc.ClientIdentification;
import io.axoniq.platform.grpc.PlatformInfo;
import io.grpc.stub.StreamObserver;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.runners.*;
import org.springframework.context.ApplicationEventPublisher;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Author: marc
 */
@RunWith(MockitoJUnitRunner.class)
public class ClusterServiceTest {
    private PlatformService testSubject;
    @Mock
    private ClusterController clusterController;

    @Mock
    private Limits limits;

    @Mock
    private ApplicationEventPublisher eventPublisher;


    private MessagingPlatformConfiguration configuration;

    @Before
    public void setUp() throws Exception {
        List<ClusterNode> nodes = new ArrayList<>();
        nodes.add(new ClusterNode("name1", "hostName1", "internalHostName1", 1, 1, 1));
        nodes.add(new ClusterNode("name2", "hostName2", "internalHostName2", 2, 2, 2));

        when(clusterController.findNodeForClient(any(), any(), any())).thenReturn(nodes.get(0));
        when(clusterController.messagingNodes()).thenReturn(nodes.stream().skip(1));
        configuration = new MessagingPlatformConfiguration(null);

        testSubject = new PlatformService(clusterController, configuration, () -> ContextController.DEFAULT, eventPublisher, limits);
    }

    @Test
    public void retrieveClusterInfo() throws Exception {
        ClientIdentification request = ClientIdentification.newBuilder()
                .setComponentName("component")
                .setClientName("client")
                .build();
        AtomicReference<PlatformInfo> clusterInfoRef = new AtomicReference<>();
        AtomicBoolean completed = new AtomicBoolean(false);
        testSubject.getPlatformServer(request, new StreamObserver<PlatformInfo>() {
            @Override
            public void onNext(PlatformInfo clusterInfo) {
                clusterInfoRef.set(clusterInfo);
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {
                completed.set(true);
            }
        });

        assertNotNull( clusterInfoRef.get());
        assertTrue(completed.get());
        assertEquals("name1", clusterInfoRef.get().getPrimary().getNodeName());
    }

}