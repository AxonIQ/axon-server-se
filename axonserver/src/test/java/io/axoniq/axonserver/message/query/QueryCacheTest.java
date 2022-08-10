/*
 *  Copyright (c) 2017-2022 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 *  under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.message.query;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.message.command.InsufficientBufferCapacityException;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;

import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class QueryCacheTest extends TestCase {

    private QueryCache testSubject;

    @Before
    public void setUp() {
        testSubject = new QueryCache(50000, 1);
    }

    @Test(expected = InsufficientBufferCapacityException.class)
    public void onFullCapacityThrowError() {

        testSubject.putIfAbsent("1234", mock(ActiveQuery.class));
        testSubject.putIfAbsent("4567", mock(ActiveQuery.class));
    }

    @Test
    public void cancelWithErrorOnTimeout() {
        QueryCache testSubject = new QueryCache(0, 1);
        ActiveQuery activeQuery = mock(ActiveQuery.class);
        QueryDefinition queryDefinition = mock(QueryDefinition.class);
        when(queryDefinition.getQueryName()).thenReturn("myQueryName");
        when(activeQuery.getQuery()).thenReturn(queryDefinition);
        when(activeQuery.getSourceClientId()).thenReturn("theRequester");
        when(activeQuery.waitingFor()).thenReturn(Collections.singleton("theResponder"));
        testSubject.putIfAbsent("myKey", activeQuery);
        testSubject.clearOnTimeout();
        verify(activeQuery).cancelWithError(eq(ErrorCode.QUERY_TIMEOUT),
                                            matches("Query cancelled due to timeout"));
    }
}