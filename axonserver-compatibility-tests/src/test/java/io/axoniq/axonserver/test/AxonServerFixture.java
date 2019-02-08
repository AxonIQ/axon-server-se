package io.axoniq.axonserver.test;

import io.axoniq.axonserver.AxonServerAccessController;
import io.axoniq.axonserver.access.jpa.PathMapping;
import io.axoniq.axonserver.config.AccessControlConfiguration;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.config.SystemInfoProvider;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.grpc.AxonServerClientService;
import io.axoniq.axonserver.grpc.Gateway;
import io.axoniq.axonserver.grpc.GrpcContextProvider;
import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
import io.axoniq.axonserver.grpc.PlatformService;
import io.axoniq.axonserver.grpc.axonhub.AxonHubEventService;
import io.axoniq.axonserver.grpc.axonhub.AxonHubPlatformService;
import io.axoniq.axonserver.grpc.control.ClientIdentification;
import io.axoniq.axonserver.grpc.control.NodeInfo;
import io.axoniq.axonserver.grpc.control.PlatformInboundInstruction;
import io.axoniq.axonserver.grpc.control.PlatformInfo;
import io.axoniq.axonserver.grpc.event.TrackingToken;
import io.axoniq.axonserver.message.event.EventDispatcher;
import io.grpc.stub.StreamObserver;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Author: marc
 */
public class AxonServerFixture {
    private Gateway gateway;
    private MessagingPlatformConfiguration configuration = new MessagingPlatformConfiguration(new SystemInfoProvider() {
        @Override
        public int getPort() {
            return 8080;
        }

        @Override
        public String getHostName() throws UnknownHostException {
            return "localhost";
        }
    });
    private AxonServerAccessController accessController;

    public void start() {
        configuration.setPort(9999);
        configuration.setAccesscontrol(new AccessControlConfiguration());
        configuration.getAccesscontrol().setEnabled(true);
        accessController = new AxonServerAccessController() {
            @Override
            public boolean allowed(String fullMethodName, String context, String token) {
                return token.equals("1234") && context.equals("SAMPLE");
            }

            @Override
            public boolean validToken(String token) {
                return token.equals("1234");
            }

            @Override
            public Collection<PathMapping> getPathMappings() {
                return null;
            }

            @Override
            public boolean isRoleBasedAuthentication() {
                return false;
            }

            @Override
            public Application getApplication(String token) {
                return null;
            }
        };

        PlatformService axonserverPlatformService = mock(PlatformService.class);
        when(axonserverPlatformService.openStream(any())).thenReturn(new StreamObserver<PlatformInboundInstruction>() {
            @Override
            public void onNext(PlatformInboundInstruction platformInboundInstruction) {

            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {

            }
        });
        doAnswer(invocationOnMock -> {
            ClientIdentification clientIdentification = (ClientIdentification)invocationOnMock.getArguments()[0];
            StreamObserver<PlatformInfo> responseStream = (StreamObserver<PlatformInfo>)invocationOnMock.getArguments()[1];
            responseStream.onNext(PlatformInfo.newBuilder().setPrimary(NodeInfo.newBuilder()
                                                                               .setNodeName(clientIdentification.getClientId())
                                                                               .setHostName("localhost")
                                                                               .setGrpcPort(configuration.getPort())
            ).build());
            responseStream.onCompleted();
            return null;
        }).when(axonserverPlatformService).getPlatformServer(any(), any());

        List<AxonServerClientService> axonhubClientServices = new ArrayList<>();
        axonhubClientServices.add(new AxonHubPlatformService(axonserverPlatformService));
        EventDispatcher axonserverEventService = mock(EventDispatcher.class);
        doAnswer(invocation -> {
            StreamObserver<TrackingToken> trackingTokenStreamObserver = (StreamObserver<TrackingToken>) invocation
                    .getArguments()[1];
            GrpcContextProvider grpcContextProvider = new GrpcContextProvider();
            if( "SAMPLE".equals(grpcContextProvider.getContext())) {
                trackingTokenStreamObserver.onNext(TrackingToken.newBuilder().setToken(1000).build());
                trackingTokenStreamObserver.onCompleted();
            } else {
                trackingTokenStreamObserver.onError(GrpcExceptionBuilder.build(ErrorCode.NO_EVENTSTORE, "No event store for: " + grpcContextProvider.getContext()));

            }
            return null;
        }).when(axonserverEventService).getFirstToken(any(), any());
        axonhubClientServices.add(new AxonHubEventService(axonserverEventService));
        gateway = new Gateway(configuration, axonhubClientServices, accessController);
        gateway.start();
    }

    public void stop() {
        if( gateway.isRunning()) {
            gateway.stop();
        }
    }

    public int getPort() {
        return configuration.getPort();
    }

}
