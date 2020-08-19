/*
 * Copyright (c) 2017-2020 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message;

import io.axoniq.axonserver.component.tags.ClientTagsCache;
import io.axoniq.axonserver.component.tags.ClientTagsUpdate;
import io.axoniq.axonserver.grpc.ClientIdRegistry;
import io.axoniq.axonserver.grpc.MetaDataValue;
import org.junit.*;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class MetaDataBasedTargetSelectorTest {

    private ClientTagsCache clientTagsCache = new ClientTagsCache(new ClientIdRegistry() {
        @Override
        public boolean register(String clientStreamId, String clientId) {
            return false;
        }

        @Override
        public void registerPlatform(String clientStreamId, String clientId) {

        }

        @Override
        public void unregisterPlatform(String clientStreamId) {

        }

        @Override
        public boolean unregister(String clientStreamId) {
            return false;
        }

        @Override
        public String clientId(String clientStreamId) {
            return clientStreamId;
        }

        @Override
        public Set<String> platformStreamIdsFor(String clientId) {
            return Collections.singleton(clientId);
        }
    });
    private MetaDataBasedTargetSelector testSubject = new MetaDataBasedTargetSelector(clientTagsCache);

    @Test
    public void findOnOneMetaDataValue() {
        clientTagsCache.on(new ClientTagsUpdate("client1", "context1", Collections.singletonMap("location", "Europe")));
        clientTagsCache.on(new ClientTagsUpdate("client2", "context1", Collections.singletonMap("location", "Asia")));
        Set<ClientStreamIdentification> clientIdentifications = new HashSet<>();
        clientIdentifications.add(new ClientStreamIdentification("context1", "client1"));
        clientIdentifications.add(new ClientStreamIdentification("context1", "client2"));
        Map<String, MetaDataValue> metaData = Collections.singletonMap("location",
                                                                       MetaDataValue.newBuilder().setTextValue("Asia")
                                                                                    .build());
        Set<ClientStreamIdentification> targets = testSubject.apply(metaData, clientIdentifications);
        assertEquals(1, targets.size());
        ClientStreamIdentification client = targets.iterator().next();
        assertEquals("client2", client.getClientStreamId());
    }

    @Test
    public void findWithoutMetaData() {
        clientTagsCache.on(new ClientTagsUpdate("client1", "context1", Collections.singletonMap("location", "Europe")));
        clientTagsCache.on(new ClientTagsUpdate("client2", "context1", Collections.singletonMap("location", "Asia")));
        Set<ClientStreamIdentification> clientIdentifications = new HashSet<>();
        clientIdentifications.add(new ClientStreamIdentification("context1", "client1"));
        clientIdentifications.add(new ClientStreamIdentification("context1", "client2"));
        Set<ClientStreamIdentification> targets = testSubject.apply(Collections.emptyMap(), clientIdentifications);
        assertEquals(2, targets.size());
    }

    @Test
    public void findClientNotInCache() {
        clientTagsCache.on(new ClientTagsUpdate("client1", "context1", Collections.singletonMap("location", "Europe")));
        clientTagsCache.on(new ClientTagsUpdate("client2", "context1", Collections.singletonMap("location", "Asia")));
        Set<ClientStreamIdentification> clientIdentifications = new HashSet<>();
        clientIdentifications.add(new ClientStreamIdentification("context1", "client3"));
        Set<ClientStreamIdentification> targets = testSubject.apply(Collections.emptyMap(), clientIdentifications);
        assertEquals(1, targets.size());
        ClientStreamIdentification client = targets.iterator().next();
        assertEquals("client3", client.getClientStreamId());
    }

    @Test
    public void findWithoutMatchingMetaData() {
        clientTagsCache.on(new ClientTagsUpdate("client1", "context1", Collections.singletonMap("location", "Europe")));
        clientTagsCache.on(new ClientTagsUpdate("client2", "context1", Collections.singletonMap("location", "Asia")));
        Set<ClientStreamIdentification> clientIdentifications = new HashSet<>();
        clientIdentifications.add(new ClientStreamIdentification("context1", "client1"));
        clientIdentifications.add(new ClientStreamIdentification("context1", "client2"));
        Map<String, MetaDataValue> metaData = Collections.singletonMap("location",
                                                                       MetaDataValue.newBuilder().setTextValue("Africa")
                                                                                    .build());
        Set<ClientStreamIdentification> targets = testSubject.apply(metaData, clientIdentifications);
        assertEquals(2, targets.size());
    }
}