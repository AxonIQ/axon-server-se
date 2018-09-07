package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.TestSystemInfoProvider;
import io.axoniq.axonserver.cluster.ClusterController;
import io.axoniq.axonserver.cluster.NodeSelectionStrategy;
import io.axoniq.axonserver.cluster.jpa.ClusterNode;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.context.ContextController;
import io.axoniq.axonserver.context.jpa.Context;
import io.axoniq.axonhub.internal.grpc.NodeInfo;
import io.axoniq.axonserver.licensing.Limits;
import io.axoniq.platform.grpc.ClientIdentification;
import io.axoniq.platform.grpc.PlatformInboundInstruction;
import io.axoniq.platform.grpc.PlatformInfo;
import io.axoniq.platform.grpc.PlatformOutboundInstruction;
import io.grpc.stub.StreamObserver;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.runners.*;
import org.springframework.context.ApplicationEventPublisher;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.*;

/**
 * Author: marc
 */
@RunWith(MockitoJUnitRunner.class)
public class PlatformServiceTest {
    private PlatformService platformService;
    @Mock
    private Limits limits;

    private ClusterController clusterController;
    @Before
    public void setUp()  {
        MessagingPlatformConfiguration configuration = new MessagingPlatformConfiguration(new TestSystemInfoProvider());
        List<ClusterNode> nodes = new ArrayList<>();
        nodes.add(new ClusterNode("MyName", "LAPTOP-1QH9GIHL.axoniq.io", "LAPTOP-1QH9GIHL.axoniq.net", 8124, 8224, 8024));


        ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);


        Context context = new Context(ContextController.DEFAULT);
        nodes.get(0).addContext(context, true, true);
        EntityManager entityManager = mock(EntityManager.class);
        when(entityManager.find(eq(Context.class), any())).thenReturn(context);
        TypedQuery<ClusterNode> mockTypedQuery = mock(TypedQuery.class);
        when(entityManager.createQuery(any(), eq(ClusterNode.class))).thenReturn(mockTypedQuery);
        when(mockTypedQuery.getResultList()).thenReturn(nodes);
        when(entityManager.find(eq(ClusterNode.class), any())).thenReturn(nodes.get(0));
        clusterController = new ClusterController(configuration, entityManager, null, new NodeSelectionStrategy() {
            @Override
            public String selectNode(String clientName, String componentName, Collection<String> activeNodes) {
                return activeNodes.iterator().next();
            }

            @Override
            public boolean canRebalance(String clientName, String componentName, List<String> activeNodes) {
                return true;
            }
        }, eventPublisher, limits);

        platformService = new PlatformService(clusterController, configuration, () -> ContextController.DEFAULT, eventPublisher, limits);
    }

    @Test
    public void getPlatformServer() {
        StreamObserver<PlatformInfo> responseObserver = new StreamObserver<PlatformInfo>() {
            @Override
            public void onNext(PlatformInfo platformInfo) {
                System.out.println( platformInfo.getPrimary());
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {

            }
        };
        ClientIdentification client = ClientIdentification.newBuilder().setClientName("client").setClientName("component").build();
        platformService.getPlatformServer(client, responseObserver);
    }

    @Test
    public void openStream() {
        StreamObserver<PlatformInboundInstruction> requestStream = platformService.openStream(new StreamObserver<PlatformOutboundInstruction>() {
            @Override
            public void onNext(PlatformOutboundInstruction platformOutboundInstruction) {
                System.out.println( platformOutboundInstruction);
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {

            }
        });
        requestStream.onNext(PlatformInboundInstruction.newBuilder().setRegister(ClientIdentification.newBuilder()
                .setClientName("client")
                .setComponentName("component")
                ).build());

        clusterController.addConnection(NodeInfo.newBuilder().setNodeName("Node1").build());

        platformService.requestReconnect("client");
    }

}