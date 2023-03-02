/*
 *  Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.localstorage.file;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.config.SystemInfoProvider;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.SerializedTransactionWithToken;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class DefaultSegmentTransformerTest {

    @Rule
    public TemporaryFolder storagePath = new TemporaryFolder();

    private DefaultSegmentTransformer testSubject;
    private Map<String, List<IndexEntry>> indexEntriesMap;

    private List<SerializedTransactionWithToken> transactions = new ArrayList<>();

    @Before
    public void setÚp() {
        StorageProperties storageProperties = new StorageProperties(new SystemInfoProvider() {
        });
        IndexManager indexManager = mock(IndexManager.class);
        doAnswer(invocationOnMock -> {
            indexEntriesMap = invocationOnMock.getArgument(2);
            return null;
        }).when(indexManager).createNewVersion(anyLong(), anyInt(), anyMap());
        TransactionIterator transactionIterator = new TransactionIterator() {
            int index = 0;

            @Override
            public boolean hasNext() {
                return index < transactions.size();
            }

            @Override
            public SerializedTransactionWithToken next() {
                return transactions.get(index++);
            }
        };
        testSubject = new DefaultSegmentTransformer(storageProperties,
                                                    0,
                                                    1,
                                                    indexManager,
                                                    () -> transactionIterator,
                                                    storagePath.getRoot().getAbsolutePath());
    }


    @Test
    public void transformEvent() {
        StepVerifier.create(testSubject.initialize()
                                       .then(testSubject.transformEvent(
                                               eventWithToken(0,
                                                              event("new-payload", "agg-0", 0))))
                                       .then(testSubject.completeSegment())
                    )
                    .expectComplete()
                    .verify();

        assertEquals(0, indexEntriesMap.size());
    }

    @Test
    public void transformFirstEvent() {
        transactions.add(serializedTransactionWithToken(0, event("old-payload", "agg-0", 0)));
        transactions.add(serializedTransactionWithToken(1, event("old-payload", "agg-1", 0),
                                                        event("old-payload", "agg-1", 1)));
        StepVerifier.create(testSubject.initialize()
                                       .then(testSubject.transformEvent(
                                               eventWithToken(0,
                                                              event("new-payload", "agg-0", 0))))
                                       .then(testSubject.completeSegment())
                    )
                    .expectComplete()
                    .verify();

        assertEquals(2, indexEntriesMap.size());
        assertEquals(3, indexEntres());
    }

    @Test
    public void deleteFirstEvent() {
        transactions.add(serializedTransactionWithToken(0, event("old-payload", "agg-0", 0)));
        transactions.add(serializedTransactionWithToken(1, event("old-payload", "agg-1", 0),
                                                        event("old-payload", "agg-1", 1)));
        StepVerifier.create(testSubject.initialize()
                                       .then(testSubject.transformEvent(
                                               eventWithToken(0,
                                                              Event.getDefaultInstance())))
                                       .then(testSubject.completeSegment())
                    )
                    .expectComplete()
                    .verify();

        assertEquals(2, indexEntriesMap.size());
        assertEquals(3, indexEntres());
    }

    @Test
    public void transformSecondEvent() {
        transactions.add(serializedTransactionWithToken(0, event("old-payload", "agg-0", 0)));
        transactions.add(serializedTransactionWithToken(1, event("old-payload", "agg-1", 0),
                                                        event("old-payload", "agg-1", 1)));
        StepVerifier.create(testSubject.initialize()
                                       .then(testSubject.transformEvent(
                                               eventWithToken(1,
                                                              event("new-payload", "agg-1", 0))))
                                       .then(testSubject.completeSegment())
                    )
                    .expectComplete()
                    .verify();

        assertEquals(2, indexEntriesMap.size());
        assertEquals(3, indexEntres());
    }

    @Test
    public void transformThirdEvent() {
        transactions.add(serializedTransactionWithToken(0, event("old-payload", "agg-0", 0)));
        transactions.add(serializedTransactionWithToken(1, event("old-payload", "agg-1", 0),
                                                        event("old-payload", "agg-1", 1)));
        StepVerifier.create(testSubject.initialize()
                                       .then(testSubject.transformEvent(
                                               eventWithToken(2,
                                                              event("new-payload", "agg-1", 1))))
                                       .then(testSubject.completeSegment())
                    )
                    .expectComplete()
                    .verify();

        assertEquals(2, indexEntriesMap.size());
        assertEquals(3, indexEntres());
    }

    private int indexEntres() {
        return indexEntriesMap.values()
                              .stream()
                              .mapToInt(List::size)
                              .reduce(Integer::sum)
                              .orElse(0);
    }

    private SerializedTransactionWithToken serializedTransactionWithToken(long token, Event... events) {
        return new SerializedTransactionWithToken(token,
                                                  (byte) 0,
                                                  Arrays.stream(events).map(e -> new SerializedEvent(e)).collect(
                                                          Collectors.toList()));
    }

    private EventWithToken eventWithToken(int token, Event event) {
        return EventWithToken.newBuilder()
                             .setToken(token).setEvent(event).build();
    }

    private Event event(String payload, String aggregateId, long seqNr) {
        return Event.newBuilder()
                    .setAggregateSequenceNumber(seqNr)
                    .setAggregateType("Type")
                    .setAggregateIdentifier(aggregateId)
                    .setPayload(SerializedObject.newBuilder().setData(ByteString.copyFromUtf8(payload)))
                    .build();
    }
}