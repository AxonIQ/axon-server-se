package io.axoniq.axonserver.config;

import org.junit.*;

import static org.junit.Assert.*;

/**
 * Author: marc
 */
public class SystemInfoProviderTest {
    private SystemInfoProvider testSubject = new SystemInfoProvider() {};
    @Test
    public void javaWithModulesOnWindows() {
        assertFalse(testSubject.javaOnWindows() && testSubject.javaWithModules());
    }
}