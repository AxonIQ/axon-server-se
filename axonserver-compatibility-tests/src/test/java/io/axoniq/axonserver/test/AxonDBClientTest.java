package io.axoniq.axonserver.test;

import io.axoniq.axondb.client.AxonDBClient;
import io.axoniq.axondb.client.AxonDBConfiguration;
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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Author: marc
 */
public class AxonDBClientTest {
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

        AxonDBConfiguration axonDBConfiguration = AxonDBConfiguration.newBuilder("localhost:" + fixture.getPort()).build();
        axonDBConfiguration.setToken("1234");
        axonDBConfiguration.setContext("SAMPLE");
        AxonDBClient axonDBClient = new AxonDBClient(axonDBConfiguration);

        io.axoniq.axondb.grpc.TrackingToken token = axonDBClient.getFirstToken().get();
        assertEquals(1000, token.getToken());
    }
}
