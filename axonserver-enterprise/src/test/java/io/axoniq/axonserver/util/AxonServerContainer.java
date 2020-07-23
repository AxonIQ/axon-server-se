package io.axoniq.axonserver.util;

import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;

/**
 * @author Marc Gathier
 */
public class AxonServerContainer {

    public static final int AXON_SERVER_HTTP_PORT = 8024;
    public static final int AXON_SERVER_GRPC_PORT = 8124;
    public static final int AXON_SERVER_INTERNAL_PORT = 8224;
    private static final long WAIT_IN_SECONDS = 90L;

    private static GenericContainer container;

    public static GenericContainer getInstance(String name, String publicHostname) {
        if (container == null) {
            container = new GenericContainer("eu.gcr.io/axoniq-devops/axonserver-enterprise:4.4-RC1-SNAPSHOT")
                    .withExposedPorts(AXON_SERVER_HTTP_PORT, AXON_SERVER_GRPC_PORT, AXON_SERVER_INTERNAL_PORT)
                    .waitingFor(Wait.forHttp("/actuator/info").forPort(AXON_SERVER_HTTP_PORT))
                    .withClasspathResourceMapping("/axonserver/axoniq.license",
                                                  "/config/axoniq.license",
                                                  BindMode.READ_ONLY)
                    .withClasspathResourceMapping("/axonserver/axonserver.properties",
                                                  "/config/axonserver.properties",
                                                  BindMode.READ_ONLY)
                    .withEnv("AXONIQ_LICENSE", "/config/axoniq.license")
                    .withEnv("AXONIQ_AXONSERVER_NAME", name)
                    .withEnv("AXONIQ_AXONSERVER_INTERNAL_HOSTNAME", name)
                    .withEnv("AXONIQ_AXONSERVER_HOSTNAME", publicHostname)
                    .withStartupTimeout(Duration.ofSeconds(WAIT_IN_SECONDS));
        }
        return container;
    }
}
