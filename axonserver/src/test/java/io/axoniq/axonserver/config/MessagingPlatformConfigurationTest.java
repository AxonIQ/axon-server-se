/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.config;

import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.unit.DataSize;

import java.lang.invoke.MethodHandles;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class MessagingPlatformConfigurationTest {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final String FIELD_NAME = "name";
    private static final String FIELD_HOSTNAME = "hostname";
    private static final String FIELD_DOMAIN = "domain";
    private static final String FIELD_FQDN = "fqdn";

    private static final String NOT_SET = null;
    private static final String DONT_TEST = "DONTTEST";
    private static final String SET_TO_EMPTY = "";
    private static final String TEST_AXONIQ_IO = "test.axoniq.io";
    private static final String TEST = "test";
    private static final String AXONIQ_IO = "axoniq.io";
    private static final String AXONSERVER_DEMO_IO = "axonserver.demo.io";
    private static final String AXONSERVER = "axonserver";
    private static final String DEMO_IO = "demo.io";
    private static final String AXONSERVER_AXONIQ_IO = "axonserver.axoniq.io";

    private MessagingPlatformConfiguration buildConfigWithHostname(final String name,
                                                                   final String hostname, final String domain,
                                                                   final String systemHostname) {
        MessagingPlatformConfiguration result = new MessagingPlatformConfiguration(new SystemInfoProvider() {
            @Override
            public String getHostName() {
                return systemHostname;
            }
        });
        result.setName(name);
        result.setHostname(hostname);
        result.setDomain(domain);
        result.postConstruct();

        return result;
    }

    private final static NameTest[] tests = {
            new NameTest("Name from hostname",
                    "When no name is set, the name is taken from the hostname.",
                    NOT_SET, NOT_SET, NOT_SET, TEST_AXONIQ_IO,
                    TEST, DONT_TEST, DONT_TEST, DONT_TEST),
            new NameTest("Name trumps hostname",
                    "When a name is set, it is used instead of the hostname",
                    AXONSERVER, NOT_SET, NOT_SET, TEST_AXONIQ_IO,
                    AXONSERVER, DONT_TEST, DONT_TEST, DONT_TEST),
            new NameTest("Hostname from OS",
                    "If nothing is set, the hostname and domain are taken from the OS.",
                    NOT_SET, NOT_SET, NOT_SET, TEST,
                    DONT_TEST, TEST, SET_TO_EMPTY, TEST),
            new NameTest("FQDN from OS",
                    "If nothing is set, the hostname and domain are taken from the OS.",
                    NOT_SET, NOT_SET, NOT_SET, TEST_AXONIQ_IO,
                    DONT_TEST, TEST, AXONIQ_IO, TEST_AXONIQ_IO),
            new NameTest("Explicit hostname",
                    "If hostname is set, it is used instead of that from the OS.",
                    NOT_SET, AXONSERVER, NOT_SET, TEST_AXONIQ_IO,
                    DONT_TEST, AXONSERVER, AXONIQ_IO, AXONSERVER_AXONIQ_IO),
            new NameTest("Explicitly empty domain",
                    "If domain is set as empty string, it is used instead of that from the OS.",
                    NOT_SET, AXONSERVER, SET_TO_EMPTY, TEST_AXONIQ_IO,
                    DONT_TEST, AXONSERVER, SET_TO_EMPTY, AXONSERVER),
            new NameTest("Explicitly domain",
                    "If domain is set, it is used instead of that from the OS.",
                    NOT_SET, AXONSERVER, DEMO_IO, TEST_AXONIQ_IO,
                    DONT_TEST, AXONSERVER, DEMO_IO, AXONSERVER_DEMO_IO)
    };

    private void testEquals(String name, String message, String fieldName, String expected, String actual) {
        if (!DONT_TEST.equals(expected)) {
            assertEquals(name + ": " + message + " [" + fieldName + "]", expected, actual);
        }
    }

    private void runTest(NameTest test) {
        logger.debug("Performing test '{}'", test.testName);

        final MessagingPlatformConfiguration conf = buildConfigWithHostname(
                test.name, test.hostname, test.domain, test.systemHostname);

        testEquals(test.testName, test.testMessage, FIELD_NAME, test.expectedName, conf.getName());
        testEquals(test.testName, test.testMessage, FIELD_HOSTNAME, test.expectedHostname, conf.getHostname());
        testEquals(test.testName, test.testMessage, FIELD_DOMAIN, test.expectedDomain, conf.getDomain());
        testEquals(test.testName, test.testMessage, FIELD_FQDN, test.expectedFQDN, conf.getFullyQualifiedHostname());
    }

    // How we define the node's name

    @Test
    public void testNames() {
        for (NameTest test : tests) {
            runTest(test);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMaxMessageSizeTooLarge() {
        MessagingPlatformConfiguration configuration = buildConfigWithHostname("a", "b", "c", "d");
        configuration.setMaxMessageSize(DataSize.ofBytes(Integer.MAX_VALUE + 1L));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMaxMessageSizeTooSmall() {
        MessagingPlatformConfiguration configuration = buildConfigWithHostname("a", "b", "c", "d");
        configuration.setMaxMessageSize(DataSize.ofBytes(-1));
    }

    private static class NameTest {

        private final String testName;
        private final String testMessage;
        private final String name;
        private final String hostname;
        private final String domain;
        private final String systemHostname;
        private final String expectedName;
        private final String expectedHostname;
        private final String expectedDomain;
        private final String expectedFQDN;

        private NameTest(String testName, String testMessage,
                         String name, String hostname, String domain, String systemHostname,
                         String expectedName, String expectedHostname, String expectedDomain, String expectedFQDN) {
            this.testName = testName;
            this.testMessage = testMessage;
            this.name = name;
            this.hostname = hostname;
            this.domain = domain;
            this.systemHostname = systemHostname;
            this.expectedName = expectedName;
            this.expectedHostname = expectedHostname;
            this.expectedDomain = expectedDomain;
            this.expectedFQDN = expectedFQDN;
        }
    }

}
