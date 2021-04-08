package io.axoniq.axonserver.refactoring.client.processor;

import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import io.axoniq.axonserver.refactoring.client.processor.listener.ClientProcessor;
import io.axoniq.axonserver.refactoring.client.processor.listener.FakeClientProcessor;
import io.axoniq.axonserver.refactoring.configuration.topology.Topology;
import org.junit.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link ClientProcessorsByComponent}
 *
 * @author Sara Pellegrini
 * @since 4.2
 */
public class ClientProcessorsByComponentTest {


    @Test
    public void test() {
        Collection<ClientProcessor> delegate = new ArrayList<>();
        ClientProcessor blueA = new FakeClientProcessor("clientA", true, Topology.DEFAULT_CONTEXT,
                                                        EventProcessorInfo.newBuilder().setProcessorName("Blue")
                                                                          .build());

        ClientProcessor greenA = new FakeClientProcessor("clientA", true, Topology.DEFAULT_CONTEXT,
                                                         EventProcessorInfo.newBuilder().setProcessorName("Green")
                                                                           .build());

        ClientProcessor redA = new FakeClientProcessor("clientA", true, Topology.DEFAULT_CONTEXT,
                                                       EventProcessorInfo.newBuilder().setProcessorName("Red").build());

        ClientProcessor blueB = new FakeClientProcessor("clientB", true, Topology.DEFAULT_CONTEXT,
                                                        EventProcessorInfo.newBuilder().setProcessorName("Blue")
                                                                          .build());

        ClientProcessor yellowB = new FakeClientProcessor("clientB", true, Topology.DEFAULT_CONTEXT,
                                                          EventProcessorInfo.newBuilder().setProcessorName("Yellow")
                                                                            .build());

        ClientProcessor greenC = new FakeClientProcessor("clientC", false, Topology.DEFAULT_CONTEXT,
                                                         EventProcessorInfo.newBuilder().setProcessorName("Green")
                                                                           .build());

        ClientProcessor yellowC = new FakeClientProcessor("clientC", false, Topology.DEFAULT_CONTEXT,
                                                          EventProcessorInfo.newBuilder().setProcessorName("Yellow")
                                                                            .build());

        ClientProcessor blackC = new FakeClientProcessor("clientC", false, Topology.DEFAULT_CONTEXT,
                                                         EventProcessorInfo.newBuilder().setProcessorName("Black")
                                                                           .build());

        ClientProcessor greenD = new FakeClientProcessor("clientD", true, "OtherContext",
                                                         EventProcessorInfo.newBuilder().setProcessorName("Green")
                                                                           .build());

        ClientProcessor blueD = new FakeClientProcessor("clientD", false, "OtherContext",
                                                        EventProcessorInfo.newBuilder().setProcessorName("Blue")
                                                                          .build());

        delegate.add(blueA);
        delegate.add(greenA);
        delegate.add(redA);
        delegate.add(blueB);
        delegate.add(yellowB);
        delegate.add(greenC);
        delegate.add(yellowC);
        delegate.add(blackC);
        delegate.add(greenD);
        delegate.add(blueD);

        ClientProcessorsByComponent testSubject = new ClientProcessorsByComponent(delegate::iterator,
                                                                                  "component",
                                                                                  Topology.DEFAULT_CONTEXT);

        Iterator<ClientProcessor> iterator = testSubject.iterator();
        assertEquals(blueA, iterator.next());
        assertEquals(greenA, iterator.next());
        assertEquals(redA, iterator.next());
        assertEquals(blueB, iterator.next());
        assertEquals(yellowB, iterator.next());
        assertEquals(greenC, iterator.next());
        assertEquals(yellowC, iterator.next());
        assertFalse(iterator.hasNext());
    }
}