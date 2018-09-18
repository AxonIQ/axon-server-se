package io.axoniq.axonserver.config;

import org.junit.*;

import java.net.UnknownHostException;

import static org.junit.Assert.*;

/**
 * Author: marc
 */
public class MessagingPlatformConfigurationTest {
    private MessagingPlatformConfiguration testSubject;

    @Before
    public void init() {
        testSubject = new MessagingPlatformConfiguration(new SystemInfoProvider() {
            @Override
            public int getPort() {
                return 0;
            }

            @Override
            public String getHostName() throws UnknownHostException {
                return "test.axoniq.io";
            }
        });
    }

    @Test
    public void getHostname() {
        assertEquals("test.axoniq.io", testSubject.getHostname());
    }

    @Test
    public void getHostnameWithDomainSet() {
        testSubject.setDomain("axoniq.io");
        assertEquals("test", testSubject.getHostname());
        assertEquals("test.axoniq.io", testSubject.getFullyQualifiedHostname());
    }
}