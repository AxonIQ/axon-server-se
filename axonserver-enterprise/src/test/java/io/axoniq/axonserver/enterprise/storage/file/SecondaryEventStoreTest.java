package io.axoniq.axonserver.enterprise.storage.file;

import io.axoniq.axonserver.config.SystemInfoProvider;
import io.axoniq.axonserver.localstorage.file.EventInformation;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.file.EmbeddedDBProperties;
import io.axoniq.axonserver.localstorage.file.EventIterator;
import io.axoniq.axonserver.localstorage.file.EventSource;
import io.axoniq.axonserver.localstorage.file.IndexManager;
import io.axoniq.axonserver.localstorage.file.InputStreamEventStore;
import io.axoniq.axonserver.localstorage.transformation.DefaultEventTransformerFactory;
import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;
import io.axoniq.axonserver.topology.Topology;
import io.axoniq.axonserver.util.TestUtils;
import org.junit.*;

import java.io.IOException;
import java.util.SortedSet;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

/**
 * @author Marc Gathier
 */
public class SecondaryEventStoreTest {
    private SecondaryEventStore testSubject;

    @Before
    public void setUp() throws IOException {
        EmbeddedDBProperties embeddedDBProperties = new EmbeddedDBProperties(new SystemInfoProvider() {});
        embeddedDBProperties.getEvent().setStorage(TestUtils
                                                           .fixPathOnWindows(InputStreamEventStore.class.getResource("/data").getFile()));
        String context = Topology.DEFAULT_CONTEXT;
        IndexManager indexManager = new IndexManager(context, embeddedDBProperties.getEvent());
        EventTransformerFactory eventTransformerFactory = new DefaultEventTransformerFactory();
        testSubject = new SecondaryEventStore(new EventTypeContext(context, EventType.EVENT), indexManager,
                                                eventTransformerFactory,
                                                embeddedDBProperties.getEvent());
        testSubject.init(false);
    }




    @Test
    public void getEventSource() {
        EventSource eventSource = testSubject.getEventSource(0).get();
        EventIterator iterator = eventSource.createEventIterator(0, 5);
        assertTrue(iterator.hasNext());
        EventInformation next = iterator.next();
        assertEquals(5, next.getToken());
        while( iterator.hasNext()) {
            next = iterator.next();
        }
        assertEquals(13, next.getToken());
    }

    @Test
    public void getSegments() {
        SortedSet<Long> segments = testSubject.getSegments();
        assertTrue(segments.contains(0L));
        assertTrue(segments.contains(14L));
        assertEquals(14, (long)segments.first());
    }

}