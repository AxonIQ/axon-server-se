package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.AxonServerAccessController;
import io.axoniq.axonserver.config.AccessControlConfiguration;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.config.SslConfiguration;
import io.axoniq.axonserver.message.event.EventDispatcher;
import org.junit.*;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Author: marc
 */
public class GatewayTest {
    private Gateway testSubject;
    private AxonServerAccessController accessController;
    private MessagingPlatformConfiguration routingConfiguration;


    @Before
    public void setUp() throws Exception {
        accessController = mock(AxonServerAccessController.class);
        routingConfiguration = new MessagingPlatformConfiguration(null);
        routingConfiguration.setPort(7023);
        routingConfiguration.setAccesscontrol(new AccessControlConfiguration());
        routingConfiguration.getAccesscontrol().setEnabled(true);
        routingConfiguration.setName("JUnit");
    }

    @Test
    public void stopWithCallback() throws Exception {
        testSubject = new Gateway(routingConfiguration, Collections.emptyList(),
                                  accessController);

        AtomicBoolean stopped = new AtomicBoolean(false);
        testSubject.start();
        testSubject.stop(() -> stopped.set(true));
        assertTrue(stopped.get());
        assertFalse(testSubject.isRunning());
        Thread.sleep(50);
    }

    @Test
    public void start() throws Exception {
        testSubject = new Gateway(routingConfiguration, Collections.emptyList(),
                accessController);

        testSubject.start();
        assertTrue(testSubject.isRunning());
        testSubject.stop();
        assertFalse(testSubject.isRunning());
        Thread.sleep(50);
    }

    @Test(expected = RuntimeException.class)
    public void startWithSslIncompleteConfiguration() throws Exception {
        routingConfiguration.setSsl(new SslConfiguration());
        routingConfiguration.getSsl().setEnabled(true);
        testSubject = new Gateway(routingConfiguration,Collections.emptyList(),
                accessController);

        testSubject.start();
    }

    @Test
    public void startWithSsl() throws Exception {
        routingConfiguration.setSsl(new SslConfiguration());
        routingConfiguration.getSsl().setEnabled(true);
        routingConfiguration.getSsl().setCertChainFile("../resources/axoniq-public.crt");
        routingConfiguration.getSsl().setPrivateKeyFile("../resources/axoniq-private.pem");
        testSubject = new Gateway(routingConfiguration, Collections.emptyList(),
                accessController);
        assertTrue(testSubject.isAutoStartup());
        testSubject.start();
        assertTrue(testSubject.isRunning());
        testSubject.stop();
        Thread.sleep(50);
    }


}