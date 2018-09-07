package io.axoniq.axonserver.grpc.internal;

import io.axoniq.axonserver.TestSystemInfoProvider;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.config.SslConfiguration;
import io.axoniq.axonserver.licensing.Limits;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.runners.*;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Author: marc
 */
@RunWith(MockitoJUnitRunner.class)
public class MessagingClusterServerTest {
    private MessagingClusterServer testSubject;
    @Mock
    private MessagingClusterService clusterService;
    private MessagingPlatformConfiguration configuration;

    @Mock
    private InternalEventStoreService internalEventStore;
    @Mock
    private DataSynchronizationMaster dataSynchronizationMaster;
    @Mock
    private Limits limits;

    @Before
    public void setUp() {
        configuration = new MessagingPlatformConfiguration(new TestSystemInfoProvider());
        configuration.setSsl(new SslConfiguration());
        testSubject = new MessagingClusterServer(configuration, clusterService, dataSynchronizationMaster, internalEventStore, limits);
        when(limits.isClusterAllowed()).thenReturn(true);
    }

    @Test(expected = RuntimeException.class)
    public void startWithoutCertificateFile() {
        configuration.getSsl().setEnabled(true);
        testSubject.start();
    }

    @Test
    public void startWithoutSsl() {
        assertTrue(testSubject.isAutoStartup());
        testSubject.start();
        assertTrue(testSubject.isRunning());
        AtomicBoolean callbackExecuted = new AtomicBoolean(false);
        testSubject.stop(() -> {callbackExecuted.set(true);});
        assertTrue(callbackExecuted.get());
    }

    @Test
    public void startWithSsl() throws Exception {
        configuration.getSsl().setEnabled(true);
        configuration.getSsl().setCertChainFile("../resources/axoniq-public.crt");
        configuration.getSsl().setPrivateKeyFile("../resources/axoniq-private.pem");
        assertTrue(testSubject.isAutoStartup());
        testSubject.start();
        assertTrue(testSubject.isRunning());
        testSubject.stop();
        Thread.sleep(50);
    }

}