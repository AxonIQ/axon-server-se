package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.TestSystemInfoProvider;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.topology.DefaultTopology;
import io.axoniq.axonserver.topology.Topology;
import io.axoniq.axonserver.grpc.control.ClientIdentification;
import io.axoniq.axonserver.grpc.control.PlatformInboundInstruction;
import io.axoniq.axonserver.grpc.control.PlatformInfo;
import io.axoniq.axonserver.grpc.control.PlatformOutboundInstruction;
import io.grpc.stub.StreamObserver;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.runners.*;
import org.springframework.context.ApplicationEventPublisher;

import static org.mockito.Mockito.*;

/**
 * Author: marc
 */
@RunWith(MockitoJUnitRunner.class)
public class PlatformServiceTest {
    private PlatformService platformService;

    private Topology clusterController;
    @Before
    public void setUp()  {
        MessagingPlatformConfiguration configuration = new MessagingPlatformConfiguration(new TestSystemInfoProvider());
        ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);
        clusterController = new DefaultTopology(configuration);
        platformService = new PlatformService(clusterController, () -> Topology.DEFAULT_CONTEXT, eventPublisher);
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

//        clusterController.addConnection(NodeInfo.newBuilder().setNodeName("Node1").build());
//
        platformService.requestReconnect("client");
    }

}