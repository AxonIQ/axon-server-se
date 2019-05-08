package io.axoniq.axonserver.test;

import io.axoniq.axonhub.client.AxonHubConfiguration;
import io.axoniq.axonhub.client.PlatformConnectionManager;
import io.axoniq.axonhub.client.event.AxonDBClient;
import org.junit.*;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
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
        axonHubConfiguration.setContext("default");
        PlatformConnectionManager platformConnectionManager = new PlatformConnectionManager(axonHubConfiguration);
        AxonDBClient axonDBClient = new AxonDBClient(axonHubConfiguration, platformConnectionManager);

        io.axoniq.axondb.grpc.TrackingToken token = axonDBClient.getFirstToken().get();
        assertEquals(1000, token.getToken());
    }
}
