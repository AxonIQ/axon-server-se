/*
 * Copyright (c) 2017-2023 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.query;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.grpc.SerializedQuery;
import io.axoniq.axonserver.grpc.query.QueryRequest;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import org.assertj.core.util.Sets;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;


class ActiveQueryTest {

    @Test
    void forward() {
        List<QueryResponse> responseList = new ArrayList<>();
        String[] completed = new String[1];
        ActiveQuery testSubject = new ActiveQuery("myKey", serializedQuery(),
                                                  responseList::add,
                                                  s -> completed[0] = s, mockedQueryHandlers());
        testSubject.forward(QueryResponse.getDefaultInstance());
        testSubject.forward(QueryResponse.getDefaultInstance());
        assertEquals(2, responseList.size());
        assertNull(completed[0]);
        testSubject.complete("client1");
        testSubject.complete("client2");
        Assertions.assertNotNull(completed[0]);
    }

    @Test
    void forwardFirstNonErrorResult() {
        List<QueryResponse> responseList = new ArrayList<>();
        String[] completed = new String[1];
        ActiveQuery testSubject = new ActiveQuery("myKey", serializedQuery(),
                                                  responseList::add,
                                                  s -> completed[0] = s, mockedQueryHandlers());
        testSubject.forward(QueryResponse.newBuilder().setErrorCode("Error").build());
        testSubject.forward(QueryResponse.getDefaultInstance());
        assertEquals(1, responseList.size());
        assertNull(completed[0]);
        testSubject.complete("client1");
        testSubject.complete("client2");
        Assertions.assertNotNull(completed[0]);
    }

    @Test
    void forwardFirstError() {
        List<QueryResponse> responseList = new ArrayList<>();
        String[] completed = new String[1];
        ActiveQuery testSubject = new ActiveQuery("myKey", serializedQuery(),
                                                  responseList::add,
                                                  s -> completed[0] = s, mockedQueryHandlers());
        testSubject.forward(QueryResponse.newBuilder().setErrorCode("Error1").build());
        testSubject.forward(QueryResponse.newBuilder().setErrorCode("Error2").build());
        testSubject.complete("client1");
        testSubject.complete("client2");
        assertEquals(1, responseList.size());
        assertNotNull(completed[0]);
        assertEquals("Error2", responseList.get(0).getErrorCode());
    }

    @Test
    void cancelWithError() {
        List<QueryResponse> responseList = new ArrayList<>();
        String[] completed = new String[1];
        ActiveQuery testSubject = new ActiveQuery("myKey",
                                                  serializedQuery(),
                                                  responseList::add,
                                                  s -> completed[0] = s, mockedQueryHandlers());

        String myErrorDescription = "My error description";
        testSubject.cancelWithError(ErrorCode.OTHER, myErrorDescription);
        assertEquals(1, responseList.size());
        QueryResponse queryResponse = responseList.get(0);
        assertTrue(queryResponse.hasErrorMessage());
        assertEquals(ErrorCode.OTHER.getCode(), queryResponse.getErrorCode());
        assertEquals(myErrorDescription, queryResponse.getErrorMessage().getMessage());
        assertNotNull(completed[0]);
    }

    private Set<QueryHandler<?>> mockedQueryHandlers() {
        QueryHandler<?> handler1 = mock(QueryHandler.class);
        when(handler1.getClientStreamId()).thenReturn("client1");
        QueryHandler<?> handler2 = mock(QueryHandler.class);
        when(handler2.getClientStreamId()).thenReturn("client2");
        return Sets.newLinkedHashSet(handler1, handler2);
    }

    private SerializedQuery serializedQuery() {
        return new SerializedQuery("context", "mySource", QueryRequest.newBuilder()
                                                                      .setQuery("queryName")
                                                                      .build());
    }
}