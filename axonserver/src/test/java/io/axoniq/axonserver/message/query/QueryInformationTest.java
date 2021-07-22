package io.axoniq.axonserver.message.query;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;


class QueryInformationTest {

    @Test
    void forward() {
        List<QueryResponse> responseList = new ArrayList<>();
        String[] completed = new String[1];
        Set<String> clientStreamIds = new HashSet<>();
        clientStreamIds.add("client1");
        clientStreamIds.add("client2");
        QueryInformation testSubject = new QueryInformation("myKey", "mySource",
                                                            new QueryDefinition("context", "queryName"),
                                                            clientStreamIds, 3, responseList::add,
                                                            s -> completed[0] = s);
        testSubject.forward("client1", QueryResponse.getDefaultInstance());
        testSubject.forward("client2", QueryResponse.getDefaultInstance());
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
        Set<String> clientStreamIds = new HashSet<>();
        clientStreamIds.add("client1");
        clientStreamIds.add("client2");
        QueryInformation testSubject = new QueryInformation("myKey", "mySource",
                                                            new QueryDefinition("context", "queryName"),
                                                            clientStreamIds, 3, responseList::add,
                                                            s -> completed[0] = s);
        testSubject.forward("client1", QueryResponse.newBuilder().setErrorCode("Error").build());
        testSubject.forward("client2", QueryResponse.getDefaultInstance());
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
        Set<String> clientStreamIds = new HashSet<>();
        clientStreamIds.add("client1");
        clientStreamIds.add("client2");
        QueryInformation testSubject = new QueryInformation("myKey", "mySource",
                                                            new QueryDefinition("context", "queryName"),
                                                            clientStreamIds, 3, responseList::add,
                                                            s -> completed[0] = s);
        testSubject.forward("client1", QueryResponse.newBuilder().setErrorCode("Error1").build());
        testSubject.forward("client2", QueryResponse.newBuilder().setErrorCode("Error2").build());
        testSubject.completed("client1");
        testSubject.completed("client2");
        assertEquals(1, responseList.size());
        assertNotNull(completed[0]);
        assertEquals("Error2", responseList.get(0).getErrorCode());
    }

    @Test
    void forwardExpectingOneResult() {
        List<QueryResponse> responseList = new ArrayList<>();
        String[] completed = new String[1];
        Set<String> clientStreamIds = new HashSet<>();
        clientStreamIds.add("client1");
        clientStreamIds.add("client2");
        QueryInformation testSubject = new QueryInformation("myKey", "mySource",
                                                            new QueryDefinition("context", "queryName"),
                                                            clientStreamIds, 1, responseList::add,
                                                            s -> completed[0] = s);
        testSubject.forward("client1", QueryResponse.getDefaultInstance());
        testSubject.forward("client2", QueryResponse.getDefaultInstance());
        assertEquals(1, responseList.size());
        assertNotNull(completed[0]);
    }

    @Test
    void cancelWithError() {
        List<QueryResponse> responseList = new ArrayList<>();
        String[] completed = new String[1];
        Set<String> clientStreamIds = new HashSet<>();
        clientStreamIds.add("client1");
        clientStreamIds.add("client2");
        QueryInformation testSubject = new QueryInformation("myKey", "mySource",
                                                            new QueryDefinition("context", "queryName"),
                                                            clientStreamIds, 1, responseList::add,
                                                            s -> completed[0] = s);

        String myErrorDescription = "My error description";
        testSubject.cancelWithError(ErrorCode.OTHER, myErrorDescription);
        assertEquals(1, responseList.size());
        QueryResponse queryResponse = responseList.get(0);
        assertTrue(queryResponse.hasErrorMessage());
        assertEquals(ErrorCode.OTHER.getCode(), queryResponse.getErrorCode());
        assertEquals(myErrorDescription, queryResponse.getErrorMessage().getMessage());
        assertNotNull(completed[0]);
    }
}