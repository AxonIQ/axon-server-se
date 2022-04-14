/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.config;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.unit.DataSize;

import java.lang.invoke.MethodHandles;

import static io.axoniq.axonserver.config.MessagingPlatformConfiguration.ALLOW_EMPTY_DOMAIN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marc Gathier
 */
public class MessagingPlatformConfigurationTest {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final String FIELD_NAME = "name";
    private static final String FIELD_HOSTNAME = "hostname";
    private static final String FIELD_DOMAIN = "domain";
    private static final String FIELD_FQDN = "fqdn";
    private static final String FIELD_INTERNAL_HOSTNAME = "internal-hostname";
    private static final String FIELD_INTERNAL_DOMAIN = "internal-domain";
    private static final String FIELD_INTERNAL_FQDN = "internal-fqdn";

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
                                                                   final String internalHostname, final String internalDomain,
                                                                   final String systemHostname,
                                                                   boolean allowEmptyDomain) {
        MessagingPlatformConfiguration result = new MessagingPlatformConfiguration(new SystemInfoProvider() {
            @Override
            public String getHostName() {
                return systemHostname;
            }
        });
        result.setName(name);
        result.setHostname(hostname);
        result.setDomain(domain);
        result.setInternalHostname(internalHostname);
        result.setInternalDomain((internalDomain));
        result.getExperimental().put(ALLOW_EMPTY_DOMAIN, allowEmptyDomain);
        result.postConstruct();

        return result;
    }

    private final static NameTest[] standardTests = {
            new NameTest("Name from hostname",
                    "When no name is set, the name is taken from the hostname.",
                    NOT_SET, NOT_SET, NOT_SET, TEST_AXONIQ_IO,
                    TEST, DONT_TEST, DONT_TEST, DONT_TEST,
                    NOT_SET, NOT_SET,
                    DONT_TEST, DONT_TEST, DONT_TEST),
            new NameTest("Name trumps hostname",
                    "When a name is set, it is used instead of the hostname",
                    AXONSERVER, NOT_SET, NOT_SET, TEST_AXONIQ_IO,
                    AXONSERVER, DONT_TEST, DONT_TEST, DONT_TEST,
                    NOT_SET, NOT_SET,
                    DONT_TEST, DONT_TEST, DONT_TEST),
            new NameTest("Hostname from OS, no internal name set",
                    "If nothing is set, the hostname and domain are taken from the OS, internal hostname same as normal.",
                    NOT_SET, NOT_SET, NOT_SET, TEST,
                    DONT_TEST, TEST, SET_TO_EMPTY, TEST,
                    NOT_SET, NOT_SET,
                    TEST, SET_TO_EMPTY, TEST),
            new NameTest("FQDN from OS, no internal name set",
                    "If nothing is set, the hostname and domain are taken from the OS, internal hostname same as normal.",
                    NOT_SET, NOT_SET, NOT_SET, TEST_AXONIQ_IO,
                    DONT_TEST, TEST, AXONIQ_IO, TEST_AXONIQ_IO,
                    NOT_SET, NOT_SET,
                    TEST, AXONIQ_IO, TEST_AXONIQ_IO),
            new NameTest("Explicit hostname, no internal name set",
                    "If hostname is set, it is used instead of that from the OS, internal hostname same as normal.",
                    NOT_SET, AXONSERVER, NOT_SET, TEST_AXONIQ_IO,
                    DONT_TEST, AXONSERVER, AXONIQ_IO, AXONSERVER_AXONIQ_IO,
                    NOT_SET, NOT_SET,
                    AXONSERVER, AXONIQ_IO, AXONSERVER_AXONIQ_IO),
            new NameTest("Explicitly empty domain, no internal name set",
                    "If domain is set as empty string, it is used instead of that from the OS, internal hostname same as normal.",
                    NOT_SET, AXONSERVER, SET_TO_EMPTY, TEST_AXONIQ_IO,
                    DONT_TEST, AXONSERVER, SET_TO_EMPTY, AXONSERVER,
                    NOT_SET, NOT_SET,
                    AXONSERVER, SET_TO_EMPTY, AXONSERVER),
            new NameTest("Explicitly set domain, no internal name set",
                    "If domain is set, it is used instead of that from the OS, internal hostname same as normal.",
                    NOT_SET, AXONSERVER, DEMO_IO, TEST_AXONIQ_IO,
                    DONT_TEST, AXONSERVER, DEMO_IO, AXONSERVER_DEMO_IO,
                    NOT_SET, NOT_SET,
                    AXONSERVER, DEMO_IO, AXONSERVER_DEMO_IO)
    };

    private final static NameTest[] emptyDomainTests = {
            new NameTest("Hostname from OS, no internal name set",
                    "If nothing is set, the hostname and domain are taken from the OS, internal hostname same as normal.",
                    NOT_SET, NOT_SET, NOT_SET, TEST,
                    DONT_TEST, TEST, SET_TO_EMPTY, TEST,
                    NOT_SET, NOT_SET,
                    TEST, SET_TO_EMPTY, TEST),
            new NameTest("FQDN from OS, no internal name set",
                    "If nothing is set, the hostname and domain are taken from the OS, internal hostname same as normal.",
                    NOT_SET, NOT_SET, NOT_SET, TEST_AXONIQ_IO,
                    DONT_TEST, TEST, AXONIQ_IO, TEST_AXONIQ_IO,
                    NOT_SET, NOT_SET,
                    TEST, AXONIQ_IO, TEST_AXONIQ_IO),
            new NameTest("Explicit hostname, no internal name set",
                    "If hostname is set, it is used instead of that from the OS, internal hostname same as normal.",
                    NOT_SET, AXONSERVER, NOT_SET, TEST_AXONIQ_IO,
                    DONT_TEST, AXONSERVER, AXONIQ_IO, AXONSERVER_AXONIQ_IO,
                    NOT_SET, NOT_SET,
                    AXONSERVER, AXONIQ_IO, AXONSERVER_AXONIQ_IO),
            new NameTest("Explicitly empty domain, no internal name set",
                    "If domain is set as empty string, it is used instead of that from the OS, internal hostname same as normal.",
                    NOT_SET, AXONSERVER, SET_TO_EMPTY, TEST_AXONIQ_IO,
                    DONT_TEST, AXONSERVER, SET_TO_EMPTY, AXONSERVER,
                    NOT_SET, NOT_SET,
                    AXONSERVER, SET_TO_EMPTY, AXONSERVER),
            new NameTest("Explicitly set domain, no internal name set",
                    "If domain is set, it is used instead of that from the OS, internal hostname same as normal.",
                    NOT_SET, AXONSERVER, DEMO_IO, TEST_AXONIQ_IO,
                    DONT_TEST, AXONSERVER, DEMO_IO, AXONSERVER_DEMO_IO,
                    NOT_SET, NOT_SET,
                    AXONSERVER, DEMO_IO, AXONSERVER_DEMO_IO),

            new NameTest("Hostname from OS, no internal name set, internal domain set to empty",
                    "If nothing is set, the hostname and domain are taken from the OS, internal hostname same as normal.",
                    NOT_SET, NOT_SET, NOT_SET, TEST,
                    DONT_TEST, TEST, SET_TO_EMPTY, TEST,
                    NOT_SET, SET_TO_EMPTY,
                    TEST, SET_TO_EMPTY, TEST),
            new NameTest("FQDN from OS, no internal name set, internal domain set to empty",
                    "If nothing is set, the hostname and domain are taken from the OS, internal hostname same as normal.",
                    NOT_SET, NOT_SET, NOT_SET, TEST_AXONIQ_IO,
                    DONT_TEST, TEST, AXONIQ_IO, TEST_AXONIQ_IO,
                    NOT_SET, SET_TO_EMPTY,
                    TEST, SET_TO_EMPTY, TEST),
            new NameTest("Explicit hostname, no internal name set, internal domain set to empty",
                    "If hostname is set, it is used instead of that from the OS, internal hostname same as normal.",
                    NOT_SET, AXONSERVER, NOT_SET, TEST_AXONIQ_IO,
                    DONT_TEST, AXONSERVER, AXONIQ_IO, AXONSERVER_AXONIQ_IO,
                    NOT_SET, SET_TO_EMPTY,
                    AXONSERVER, SET_TO_EMPTY, AXONSERVER),
            new NameTest("Explicitly empty domain, no internal name set, internal domain set to empty",
                    "If domain is set as empty string, it is used instead of that from the OS, internal hostname same as normal.",
                    NOT_SET, AXONSERVER, SET_TO_EMPTY, TEST_AXONIQ_IO,
                    DONT_TEST, AXONSERVER, SET_TO_EMPTY, AXONSERVER,
                    NOT_SET, SET_TO_EMPTY,
                    AXONSERVER, SET_TO_EMPTY, AXONSERVER),
            new NameTest("Explicitly set domain, no internal name set, internal domain set to empty",
                    "If domain is set, it is used instead of that from the OS, internal hostname same as normal.",
                    NOT_SET, AXONSERVER, DEMO_IO, TEST_AXONIQ_IO,
                    DONT_TEST, AXONSERVER, DEMO_IO, AXONSERVER_DEMO_IO,
                    NOT_SET, SET_TO_EMPTY,
                    AXONSERVER, SET_TO_EMPTY, AXONSERVER)
    };

    private void testEquals(String name, String message, String fieldName, String expected, String actual) {
        if (!DONT_TEST.equals(expected)) {
            if (SET_TO_EMPTY.equals(expected)) {
                assertTrue(name + ": " + message + " [" + fieldName + "]", (NOT_SET == actual) || SET_TO_EMPTY.equals(actual));
            } else {
                assertEquals(name + ": " + message + " [" + fieldName + "]", expected, actual);
            }
        }
    }

    private static String value(String s) {
        return (s == null) ? "null" : ("\"" + s + "\"");
    }

    @Test
    public void testNames() {
        for (NameTest test : standardTests) {
            final MessagingPlatformConfiguration conf = buildConfigWithHostname(
                    test.name, test.hostname, test.domain, test.internalHostname, test.internalDomain, test.systemHostname, false);

            logger.info("{}: [{}, {}, {}, {}, {}] => [{}, {}, {}, {}, {}]", test.testName,
                    value(test.name), value(test.hostname), value(test.domain), value(test.internalHostname), value(test.internalDomain),
                    value(conf.getName()), value(conf.getHostname()), value(conf.getDomain()), value(conf.getInternalHostname()), value(conf.getInternalDomain()));

            testEquals(test.testName, test.testMessage, FIELD_NAME, test.expectedName, conf.getName());

            testEquals(test.testName, test.testMessage, FIELD_HOSTNAME, test.expectedHostname, conf.getHostname());
            testEquals(test.testName, test.testMessage, FIELD_DOMAIN, test.expectedDomain, conf.getDomain());
            testEquals(test.testName, test.testMessage, FIELD_FQDN, test.expectedFQDN, conf.getFullyQualifiedHostname());

            testEquals(test.testName, test.testMessage, FIELD_INTERNAL_HOSTNAME, test.expectedInternalHostname, conf.getInternalHostname());
            testEquals(test.testName, test.testMessage, FIELD_INTERNAL_DOMAIN, test.expectedInternalDomain, conf.getInternalDomain());
            testEquals(test.testName, test.testMessage, FIELD_INTERNAL_FQDN, test.expectedInternalFQDN, conf.getFullyQualifiedInternalHostname());
        }
    }

    @Test
    public void testNamesEmptyDomainAllowed() {
        logger.info("Test hostnames/domain WITH empty domains allowed");

        for (NameTest test : emptyDomainTests) {
            final MessagingPlatformConfiguration conf = buildConfigWithHostname(
                    test.name, test.hostname, test.domain, test.internalHostname, test.internalDomain, test.systemHostname, true);

            logger.info("{}: [{}, {}, {}, {}, {}] => [{}, {}, {}, {}, {}]", test.testName,
                    value(test.name), value(test.hostname), value(test.domain), value(test.internalHostname), value(test.internalDomain),
                    value(conf.getName()), value(conf.getHostname()), value(conf.getDomain()), value(conf.getInternalHostname()), value(conf.getInternalDomain()));

            testEquals(test.testName, test.testMessage, FIELD_NAME, test.expectedName, conf.getName());

            testEquals(test.testName, test.testMessage, FIELD_HOSTNAME, test.expectedHostname, conf.getHostname());
            testEquals(test.testName, test.testMessage, FIELD_DOMAIN, test.expectedDomain, conf.getDomain());
            testEquals(test.testName, test.testMessage, FIELD_FQDN, test.expectedFQDN, conf.getFullyQualifiedHostname());

            testEquals(test.testName, test.testMessage, FIELD_INTERNAL_HOSTNAME, test.expectedInternalHostname, conf.getInternalHostname());
            testEquals(test.testName, test.testMessage, FIELD_INTERNAL_DOMAIN, test.expectedInternalDomain, conf.getInternalDomain());
            testEquals(test.testName, test.testMessage, FIELD_INTERNAL_FQDN, test.expectedInternalFQDN, conf.getFullyQualifiedInternalHostname());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMaxMessageSizeTooLarge() {
        MessagingPlatformConfiguration configuration = buildConfigWithHostname("a", "b", "c", "d", "e", "f", true);
        configuration.setMaxMessageSize(DataSize.ofBytes(Integer.MAX_VALUE + 1L));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMaxMessageSizeTooSmall() {
        MessagingPlatformConfiguration configuration = buildConfigWithHostname("a", "b", "c", "d", "e", "f", true);
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

        private final String internalHostname;
        private final String internalDomain;
        private final String expectedInternalHostname;
        private final String expectedInternalDomain;
        private final String expectedInternalFQDN;

        private NameTest(String testName, String testMessage,
                                 String name, String hostname, String domain, String systemHostname,
                                 String expectedName, String expectedHostname, String expectedDomain, String expectedFQDN,
                                 String internalHostname, String internalDomain,
                                 String expectedInternalHostname, String expectedInternalDomain, String expectedInternalFQDN) {
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

            this.internalHostname = internalHostname;
            this.internalDomain = internalDomain;
            this.expectedInternalHostname = expectedInternalHostname;
            this.expectedInternalDomain = expectedInternalDomain;
            this.expectedInternalFQDN = expectedInternalFQDN;
        }
    }

}
