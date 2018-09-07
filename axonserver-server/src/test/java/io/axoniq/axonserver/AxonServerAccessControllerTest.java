package io.axoniq.axonserver;

import io.axoniq.axonserver.config.AccessControlConfiguration;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.licensing.Limits;
import io.axoniq.platform.application.AccessController;
import io.axoniq.platform.application.PathMappingRepository;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.runners.*;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * Author: marc
 */
@RunWith(MockitoJUnitRunner.class)
public class AxonServerAccessControllerTest {
    private AxonServerAccessController testSubject;
    @Mock
    private Limits limits;

    @Mock
    private AccessController accessController;

    @Mock
    private MessagingPlatformConfiguration messagingPlatformConfiguration;
    @Mock
    private PathMappingRepository pathMappingRepository;

    @Before
    public void setup() {
        testSubject = new AxonServerAccessController(accessController, pathMappingRepository, messagingPlatformConfiguration, limits);
        when(accessController.authorize(eq("1"), any(),any(), eq(true))).thenReturn(true);
        when(accessController.authorize(eq("2"), any(),any(), eq(false))).thenReturn(false);
        when(accessController.validToken("1")).thenReturn(true);
        AccessControlConfiguration accessControlConfiguation = new AccessControlConfiguration();
        accessControlConfiguation.setToken("3");
        when(messagingPlatformConfiguration.getAccesscontrol()).thenReturn(accessControlConfiguation);
        when(limits.isEnterprise()).thenReturn(true);
    }
    @Test
    public void allowed() {
        assertTrue(testSubject.allowed("/v1/commands", "default", "1"));
    }

    @Test
    public void notAllowed() {
        assertFalse(testSubject.allowed("/v1/commands", "default", "2"));
    }
    @Test
    public void allowedDevelopment() {
        when(limits.isEnterprise()).thenReturn(false);
        assertTrue(testSubject.allowed("/v1/commands", "default", "3"));
    }
    @Test
    public void notAllowedDevelopment() {
        when(limits.isEnterprise()).thenReturn(false);
        assertFalse(testSubject.allowed("/v1/commands", "default", "4"));
    }

    @Test
    public void validToken() {
        assertTrue(testSubject.validToken("1"));
    }
    @Test
    public void invalidToken() {
        assertFalse(testSubject.validToken("2"));
    }

    @Test
    public void validTokenDevelopment() {
        when(limits.isEnterprise()).thenReturn(false);
        assertTrue(testSubject.validToken("3"));
    }
    @Test
    public void invalidTokenDevelopment() {
        when(limits.isEnterprise()).thenReturn(false);
        assertFalse(testSubject.validToken("4"));
    }
}