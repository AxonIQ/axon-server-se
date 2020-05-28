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

    private ClientTagsCache clientTagsCache = new ClientTagsCache();
    private MetaDataBasedTargetSelector testSubject = new MetaDataBasedTargetSelector(clientTagsCache);

    @Test
    public void findOnOneMetaDataValue() {
        clientTagsCache.on(new ClientTagsUpdate("client1", "context1", Collections.singletonMap("location", "Europe")));
        clientTagsCache.on(new ClientTagsUpdate("client2", "context1", Collections.singletonMap("location", "Asia")));
        Set<ClientIdentification> clientIdentifications = new HashSet<>();
        clientIdentifications.add(new ClientIdentification("context1", "client1"));
        clientIdentifications.add(new ClientIdentification("context1", "client2"));
        Map<String, MetaDataValue> metaData = Collections.singletonMap("location",
                                                                       MetaDataValue.newBuilder().setTextValue("Asia")
                                                                                    .build());
        Set<ClientIdentification> targets = testSubject.apply(metaData, clientIdentifications);
        assertEquals(1, targets.size());
        ClientIdentification client = targets.iterator().next();
        assertEquals("client2", client.getClient());
    }

    @Test
    public void findWithoutMetaData() {
        clientTagsCache.on(new ClientTagsUpdate("client1", "context1", Collections.singletonMap("location", "Europe")));
        clientTagsCache.on(new ClientTagsUpdate("client2", "context1", Collections.singletonMap("location", "Asia")));
        Set<ClientIdentification> clientIdentifications = new HashSet<>();
        clientIdentifications.add(new ClientIdentification("context1", "client1"));
        clientIdentifications.add(new ClientIdentification("context1", "client2"));
        Set<ClientIdentification> targets = testSubject.apply(Collections.emptyMap(), clientIdentifications);
        assertEquals(2, targets.size());
    }

    @Test
    public void findWithoutMatchingMetaData() {
        clientTagsCache.on(new ClientTagsUpdate("client1", "context1", Collections.singletonMap("location", "Europe")));
        clientTagsCache.on(new ClientTagsUpdate("client2", "context1", Collections.singletonMap("location", "Asia")));
        Set<ClientIdentification> clientIdentifications = new HashSet<>();
        clientIdentifications.add(new ClientIdentification("context1", "client1"));
        clientIdentifications.add(new ClientIdentification("context1", "client2"));
        Map<String, MetaDataValue> metaData = Collections.singletonMap("location",
                                                                       MetaDataValue.newBuilder().setTextValue("Africa")
                                                                                    .build());
        Set<ClientIdentification> targets = testSubject.apply(metaData, clientIdentifications);
        assertEquals(2, targets.size());
    }
}