package io.axoniq.axonserver.component.processor;

import io.axoniq.axonserver.applicationevents.EventProcessorEvents;
import org.junit.*;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link EventProcessorResultListener}.
 *
 * @author Sara Pellegrini
 */
public class EventProcessorResultListenerTest {

    private final String context = "context";

    private final List<EventProcessorIdentifier> refreshed = new ArrayList<>();

    private final EventProcessorResultListener testSubject =
            new EventProcessorResultListener(refreshed::add,
                                             (client, processor) -> new EventProcessorIdentifier(processor, ""));

    @Before
    public void setUp() throws Exception {
        refreshed.clear();
    }

    @Test
    public void onSplit() {
        assertTrue(refreshed.isEmpty());
        testSubject.on(new EventProcessorEvents.SplitSegmentsSucceeded("clientA", "ProcessorA"));
        assertEquals(refreshed, singletonList(new EventProcessorIdentifier("ProcessorA", "")));
    }

    @Test
    public void onMerge() {
        assertTrue(refreshed.isEmpty());
        testSubject.on(new EventProcessorEvents.MergeSegmentsSucceeded("clientB", "ProcessorB"));
        assertEquals(refreshed, singletonList(new EventProcessorIdentifier("ProcessorB", "")));
    }
}