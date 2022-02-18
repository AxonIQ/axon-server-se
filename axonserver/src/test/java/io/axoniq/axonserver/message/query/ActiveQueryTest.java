package io.axoniq.axonserver.message.query;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import org.assertj.core.util.Sets;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;


class ActiveQueryTest {

    @Test
    void forward() {
        List<QueryResponse> responseList = new ArrayList<>();
        String[] completed = new String[1];
        ActiveQuery testSubject = new ActiveQuery("myKey", "mySource",
                                                  new QueryDefinition("context", "queryName"),
                                                  responseList::add,
                                                            s -> completed[0] = s, mockedQueryHandlers());
        testSubject.forward(QueryResponse.getDefaultInstance());
        testSubject.forward(QueryResponse.getDefaultInstance());
        assertEquals(2, responseList.size());
        assertNull(completed[0]);
        testSubject.completed("client1");
        testSubject.completed("client2");
        Assertions.assertNotNull(completed[0]);
    }

    @Test
    void forwardFirstNonErrorResult() {
        List<QueryResponse> responseList = new ArrayList<>();
        String[] completed = new String[1];
        ActiveQuery testSubject = new ActiveQuery("myKey", "mySource",
                                                  new QueryDefinition("context", "queryName"),
                                                  responseList::add,
                                                            s -> completed[0] = s, mockedQueryHandlers());
        testSubject.forward(QueryResponse.newBuilder().setErrorCode("Error").build());
        testSubject.forward(QueryResponse.getDefaultInstance());
        assertEquals(1, responseList.size());
        assertNull(completed[0]);
        testSubject.completed("client1");
        testSubject.completed("client2");
        Assertions.assertNotNull(completed[0]);
    }
    @Test
    void forwardFirstError() {
        List<QueryResponse> responseList = new ArrayList<>();
        String[] completed = new String[1];
        ActiveQuery testSubject = new ActiveQuery("myKey", "mySource",
                                                  new QueryDefinition("context", "queryName"),
                                                  responseList::add,
                                                            s -> completed[0] = s, mockedQueryHandlers());
        testSubject.forward(QueryResponse.newBuilder().setErrorCode("Error1").build());
        testSubject.forward(QueryResponse.newBuilder().setErrorCode("Error2").build());
        testSubject.completed("client1");
        testSubject.completed("client2");
        assertEquals(1, responseList.size());
        assertNotNull(completed[0]);
        assertEquals("Error2", responseList.get(0).getErrorCode());
    }

    @Test
    void cancelWithError() {
        List<QueryResponse> responseList = new ArrayList<>();
        String[] completed = new String[1];
        ActiveQuery testSubject = new ActiveQuery("myKey", "mySource",
                                                  new QueryDefinition("context", "queryName"),
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
}