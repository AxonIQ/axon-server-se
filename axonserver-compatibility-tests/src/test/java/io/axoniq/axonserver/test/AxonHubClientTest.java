package io.axoniq.axonserver.test;

import io.axoniq.axonhub.client.AxonHubConfiguration;
import io.axoniq.axonhub.client.PlatformConnectionManager;
import io.axoniq.axonhub.client.event.AxonDBClient;
import io.axoniq.axonserver.AxonServerAccessController;
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
import io.axoniq.platform.application.jpa.Application;
import io.axoniq.platform.application.jpa.PathMapping;
import io.grpc.stub.StreamObserver;
import org.junit.*;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Author: marc
 */
public class AxonHubClientTest {
    private AxonServerFixture fixture = new AxonServerFixture();

    @Before
    public void start() {
        fixture.start();
    }

    @After
    public void stop() {
        fixture.stop();
    }

    @Test
    public void platformServiceTest() throws ExecutionException, InterruptedException {

        AxonHubConfiguration axonHubConfiguration = AxonHubConfiguration.newBuilder("localhost:" + fixture.getPort(), "Sample").build();
        axonHubConfiguration.setToken("1234");
        axonHubConfiguration.setContext("SAMPLE");
        PlatformConnectionManager platformConnectionManager = new PlatformConnectionManager(axonHubConfiguration);
        AxonDBClient axonDBClient = new AxonDBClient(axonHubConfiguration, platformConnectionManager);

        io.axoniq.axondb.grpc.TrackingToken token = axonDBClient.getFirstToken().get();
        assertEquals(1000, token.getToken());
    }
}
