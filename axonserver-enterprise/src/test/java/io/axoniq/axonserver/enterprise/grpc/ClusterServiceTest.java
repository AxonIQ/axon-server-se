package io.axoniq.axonserver.enterprise.grpc;

import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.grpc.PlatformService;
import io.axoniq.axonserver.licensing.Limits;
import io.axoniq.axonserver.topology.AxonServerNode;
import io.axoniq.axonserver.topology.Topology;
import io.axoniq.axonserver.grpc.control.ClientIdentification;
import io.axoniq.axonserver.grpc.control.PlatformInfo;
import io.grpc.stub.StreamObserver;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.runners.*;
import org.mockito.stubbing.*;
import org.springframework.context.ApplicationEventPublisher;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.mockito.Matchers.any;

/**
 * Author: marc
 */
@RunWith(MockitoJUnitRunner.class)
public class ClusterServiceTest {
    private PlatformService testSubject;
    @Mock
    private Topology clusterController;

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

        Mockito.when(clusterController.findNodeForClient(Matchers.any(), Matchers.any(), Matchers.any())).thenReturn(nodes.get(0));
        Mockito.when(clusterController.messagingNodes()).then((Answer<Stream<? extends AxonServerNode>>) invocationOnMock -> nodes.stream().skip(1));
        configuration = new MessagingPlatformConfiguration(null);

        testSubject = new PlatformService(clusterController, () -> Topology.DEFAULT_CONTEXT, eventPublisher);
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

        Assert.assertNotNull(clusterInfoRef.get());
        Assert.assertTrue(completed.get());
        Assert.assertEquals("name1", clusterInfoRef.get().getPrimary().getNodeName());
    }

}