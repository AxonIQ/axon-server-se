package io.axoniq.axonserver.component.processor;

import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.component.processor.listener.FakeClientProcessor;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import org.junit.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import static org.junit.Assert.*;

/**
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

        delegate.add(new FakeClientProcessor("clientA", true, true, blue));
        delegate.add(new FakeClientProcessor("clientA", true, true, green));
        delegate.add(new FakeClientProcessor("clientA", true, true, red));
        delegate.add(new FakeClientProcessor("clientB", true, true, blue));
        delegate.add(new FakeClientProcessor("clientB", true, true, yellow));
        delegate.add(new FakeClientProcessor("clientC", false, true, green));
        delegate.add(new FakeClientProcessor("clientC", false, true, yellow));
        delegate.add(new FakeClientProcessor("clientC", false, true, black));
        delegate.add(new FakeClientProcessor("clientD", false, false, green));
        delegate.add(new FakeClientProcessor("clientD", false, false, blue));

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