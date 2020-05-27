package io.axoniq.axonserver.enterprise.cluster.events;

import io.axoniq.axonserver.applicationevents.AxonServerEvent;
import org.junit.*;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link ClusterEventPublisher}
 *
 * @author Sara Pellegrini
 */
public class ClusterEventPublisherTest {

    private List<Object> locallyPublished = new ArrayList<>();
    private List<AxonServerEvent> remotelyPublished = new ArrayList<>();

    private ClusterEventPublisher testSubject = new ClusterEventPublisher(remotelyPublished::add,
                                                                          locallyPublished::add);

    @Before
    public void setUp() throws Exception {
        locallyPublished.clear();
        remotelyPublished.clear();
    }

    @Test
    public void publishLocalEvent() {
        class MyLocalEvent {

        }
        MyLocalEvent event = new MyLocalEvent();
        testSubject.publishEvent(event);
        assertTrue(remotelyPublished.isEmpty());
        assertFalse(locallyPublished.isEmpty());
        assertEquals(locallyPublished.get(0), event);
    }

    @Test
    public void publishClusterEvent() {
        class MyClusterEvent implements AxonServerEvent {

        }
        MyClusterEvent event = new MyClusterEvent();
        testSubject.publishEvent(event);
        assertFalse(remotelyPublished.isEmpty());
        assertEquals(remotelyPublished.get(0), event);
        assertFalse(locallyPublished.isEmpty());
        assertEquals(locallyPublished.get(0), event);
    }
}