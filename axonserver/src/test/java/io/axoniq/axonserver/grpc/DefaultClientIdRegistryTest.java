/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.serializer.GsonMedia;
import org.junit.*;

import java.util.Set;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class DefaultClientIdRegistryTest {

    private final DefaultClientIdRegistry testSubject = new DefaultClientIdRegistry();

    @Test
    public void streamIdFor() {
        testSubject.register("clientStreamId", "clientId", ClientIdRegistry.ConnectionType.COMMAND);
        assertEquals("clientStreamId", testSubject.streamIdFor("clientId", ClientIdRegistry.ConnectionType.COMMAND));
        try {
            testSubject.streamIdFor("clientId", ClientIdRegistry.ConnectionType.QUERY);
            fail("should not find query stream");
        } catch (IllegalStateException illegalStateException) {

        }
    }

    @Test
    public void unregister() {
        testSubject.register("clientStreamId", "clientId", ClientIdRegistry.ConnectionType.COMMAND);
        testSubject.unregister("clientStreamId", ClientIdRegistry.ConnectionType.COMMAND);
        try {
            testSubject.clientId("clientStreamId");
            fail("Should not get the clientId");
        } catch (IllegalStateException illegalStateException) {
        }
        try {
            testSubject.streamIdFor("clientId", ClientIdRegistry.ConnectionType.COMMAND);
            fail("should not find command stream");
        } catch (IllegalStateException illegalStateException) {

        }

        testSubject.unregister("clientStreamId", ClientIdRegistry.ConnectionType.COMMAND);
    }

    @Test
    public void streamIdsFor() {
        testSubject.register("clientStreamId", "clientId", ClientIdRegistry.ConnectionType.COMMAND);
        testSubject.register("clientStreamId2", "clientId", ClientIdRegistry.ConnectionType.COMMAND);
        Set<String> streamIds = testSubject.streamIdsFor("clientId", ClientIdRegistry.ConnectionType.COMMAND);
        assertTrue(streamIds.contains("clientStreamId"));
        assertTrue(streamIds.contains("clientStreamId2"));
    }

    @Test
    public void printOn() {
        testSubject.register("clientStreamId", "clientId", ClientIdRegistry.ConnectionType.COMMAND);
        testSubject.register("clientStreamId2", "clientId", ClientIdRegistry.ConnectionType.QUERY);
        testSubject.register("clientStreamId3", "clientId", ClientIdRegistry.ConnectionType.PLATFORM);
        GsonMedia media = new GsonMedia();
        testSubject.printOn(media);
        System.out.println(media);
    }
}