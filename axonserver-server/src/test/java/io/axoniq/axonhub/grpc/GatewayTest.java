package io.axoniq.axonhub.grpc;

import io.axoniq.axonhub.AxonHubAccessController;
import io.axoniq.axonhub.config.AccessControlConfiguration;
import io.axoniq.axonhub.config.MessagingPlatformConfiguration;
import io.axoniq.axonhub.config.SslConfiguration;
import io.axoniq.axonhub.message.event.EventDispatcher;
import org.junit.*;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Author: marc
 */
public class GatewayTest {
    private Gateway testSubject;
    private AxonHubAccessController accessController;
    private PlatformService instructionService;
    private QueryService queryService;
    private CommandService commandService;
    private EventDispatcher eventDispatcher;
    private MessagingPlatformConfiguration routingConfiguration;

    @Before
    public void setUp() throws Exception {
        accessController = mock(AxonHubAccessController.class);
        instructionService = mock(PlatformService.class);
        queryService = mock(QueryService.class);
        commandService = mock(CommandService.class);
        eventDispatcher = mock(EventDispatcher.class);
        routingConfiguration = new MessagingPlatformConfiguration(null);
        routingConfiguration.setPort(7023);
        routingConfiguration.setAccesscontrol(new AccessControlConfiguration());
        routingConfiguration.getAccesscontrol().setEnabled(true);
        routingConfiguration.setName("JUnit");
    }

    @Test
    public void stopWithCallback() throws Exception {
        testSubject = new Gateway(routingConfiguration, eventDispatcher, commandService, queryService, instructionService,
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
        testSubject = new Gateway(routingConfiguration, eventDispatcher, commandService, queryService, instructionService,
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
        testSubject = new Gateway(routingConfiguration, eventDispatcher, commandService, queryService, instructionService,
                accessController);

        testSubject.start();
    }

    @Test
    public void startWithSsl() throws Exception {
        routingConfiguration.setSsl(new SslConfiguration());
        routingConfiguration.getSsl().setEnabled(true);
        routingConfiguration.getSsl().setCertChainFile("../resources/axoniq-public.crt");
        routingConfiguration.getSsl().setPrivateKeyFile("../resources/axoniq-private.pem");
        testSubject = new Gateway(routingConfiguration, eventDispatcher, commandService, queryService, instructionService,
                accessController);
        assertTrue(testSubject.isAutoStartup());
        testSubject.start();
        assertTrue(testSubject.isRunning());
        testSubject.stop();
        Thread.sleep(50);
    }


}