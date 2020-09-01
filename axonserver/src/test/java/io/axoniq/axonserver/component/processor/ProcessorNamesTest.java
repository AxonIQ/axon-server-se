package io.axoniq.axonserver.component.processor;

import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.component.processor.listener.FakeClientProcessor;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import io.axoniq.axonserver.topology.Topology;
import org.junit.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link ProcessorNames}
 * @author Sara Pellegrini
 * @since 4.2
 */
public class ProcessorNamesTest {

    @Test
    public void test() {
        Collection<ClientProcessor> delegate = new ArrayList<>();
        EventProcessorInfo blue = EventProcessorInfo.newBuilder().setProcessorName("Blue").build();
        EventProcessorInfo green = EventProcessorInfo.newBuilder().setProcessorName("Green").build();
        EventProcessorInfo red = EventProcessorInfo.newBuilder().setProcessorName("Red").build();
        EventProcessorInfo yellow = EventProcessorInfo.newBuilder().setProcessorName("Yellow").build();
        EventProcessorInfo black = EventProcessorInfo.newBuilder().setProcessorName("Black").build();

        delegate.add(new FakeClientProcessor("clientA", true, Topology.DEFAULT_CONTEXT, blue));
        delegate.add(new FakeClientProcessor("clientA", true, Topology.DEFAULT_CONTEXT, green));
        delegate.add(new FakeClientProcessor("clientA", true, Topology.DEFAULT_CONTEXT, red));
        delegate.add(new FakeClientProcessor("clientB", true, Topology.DEFAULT_CONTEXT, blue));
        delegate.add(new FakeClientProcessor("clientB", true, Topology.DEFAULT_CONTEXT, yellow));
        delegate.add(new FakeClientProcessor("clientC", false, Topology.DEFAULT_CONTEXT, green));
        delegate.add(new FakeClientProcessor("clientC", false, Topology.DEFAULT_CONTEXT, yellow));
        delegate.add(new FakeClientProcessor("clientC", false, Topology.DEFAULT_CONTEXT, black));
        delegate.add(new FakeClientProcessor("clientD", false, Topology.DEFAULT_CONTEXT, green));
        delegate.add(new FakeClientProcessor("clientD", false, Topology.DEFAULT_CONTEXT, blue));

        ProcessorNames testSubject = new ProcessorNames(delegate);

        Iterator<String> iterator = testSubject.iterator();
        assertEquals(blue.getProcessorName(), iterator.next());
        assertEquals(green.getProcessorName(), iterator.next());
        assertEquals(red.getProcessorName(), iterator.next());
        assertEquals(blue.getProcessorName(), iterator.next());
        assertEquals(yellow.getProcessorName(), iterator.next());
        assertEquals(green.getProcessorName(), iterator.next());
        assertEquals(yellow.getProcessorName(), iterator.next());
        assertEquals(black.getProcessorName(), iterator.next());
        assertEquals(green.getProcessorName(), iterator.next());
        assertEquals(blue.getProcessorName(), iterator.next());
        assertFalse(iterator.hasNext());
    }
}