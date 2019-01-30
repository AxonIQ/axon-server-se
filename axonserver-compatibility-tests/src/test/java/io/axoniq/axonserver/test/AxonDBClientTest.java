package io.axoniq.axonserver.test;

import io.axoniq.axondb.client.AxonDBClient;
import io.axoniq.axondb.client.AxonDBConfiguration;
import org.junit.*;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

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
