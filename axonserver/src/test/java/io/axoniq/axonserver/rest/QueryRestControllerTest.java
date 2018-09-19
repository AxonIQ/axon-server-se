package io.axoniq.axonserver.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.axoniq.axonserver.component.query.Query;
import io.axoniq.axonserver.grpc.query.QuerySubscription;
import io.axoniq.axonserver.message.query.DirectQueryHandler;
import io.axoniq.axonserver.message.query.QueryDefinition;
import io.axoniq.axonserver.message.query.QueryRegistrationCache;
import io.axoniq.axonserver.serializer.GsonMedia;
import io.axoniq.axonserver.topology.Topology;
import io.axoniq.axonserver.util.CountingStreamObserver;
import org.junit.*;

import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Author: marc
 */
public class QueryRestControllerTest {
    private QueryRestController testSubject;
    private QueryRegistrationCache registationCache;

    @Before
    public void setUp() throws Exception {
        registationCache = new QueryRegistrationCache(null);
        QuerySubscription querySubscription = QuerySubscription.newBuilder()
                .setQuery("Request")
                .setComponentName("Component")
                .setClientName("client")
                .setNrOfHandlers(1).build();
        registationCache.add(new QueryDefinition(Topology.DEFAULT_CONTEXT, querySubscription), "Response",
                             new DirectQueryHandler(new CountingStreamObserver<>(), querySubscription.getClientName(), querySubscription.getComponentName()));

        testSubject = new QueryRestController(registationCache);
    }

    @Test
    public void get() throws Exception {
        List<QueryRestController.JsonQueryMapping> queries = testSubject.get();
        assertNotEquals("[]", new ObjectMapper().writeValueAsString(queries));
    }

    @Test
    public void getByComponent(){
        Iterator<Query> iterator = testSubject.getByComponent("Component", Topology.DEFAULT_CONTEXT).iterator();
        assertTrue(iterator.hasNext());
        GsonMedia media = new GsonMedia();
        iterator.next().printOn(media);
        assertEquals("{\"name\":\"Request\",\"responseTypes\":[\"Response\"]}", media.toString());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void getByNotExistingComponent(){
        Iterator iterator = testSubject.getByComponent("otherComponent", Topology.DEFAULT_CONTEXT).iterator();
        assertFalse(iterator.hasNext());
    }

    @Test
    public void getByNotExistingContext(){
        Iterator iterator = testSubject.getByComponent("Component", "Dummy").iterator();
        assertFalse(iterator.hasNext());
    }
}