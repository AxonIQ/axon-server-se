package io.axoniq.axonserver.component.processor;

import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.component.processor.listener.FakeClientProcessor;
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
        String clientA = "clientA";
        String clientB = "clientB";
        String clientC = "clientC";
        String clientD = "clientD";
        String clientE = "clientE";

        String blue = "Blue";
        String green = "Green";
        String red = "Red";
        String yellow = "Yellow";
        String black = "Black";

        ClientProcessor blueA = new FakeClientProcessor(clientA, true, blue);
        ClientProcessor greenA = new FakeClientProcessor(clientA, true, green);
        ClientProcessor redA = new FakeClientProcessor(clientA, true, red);
        ClientProcessor blueB = new FakeClientProcessor(clientB, true, blue);
        ClientProcessor yellowB = new FakeClientProcessor(clientB, true, yellow);
        ClientProcessor greenD = new FakeClientProcessor(clientD, true, green);

        ClientProcessor greenC = new FakeClientProcessor(clientC, false, green);
        ClientProcessor yellowC = new FakeClientProcessor(clientC, false, yellow);
        ClientProcessor blackC = new FakeClientProcessor(clientC, false, black);
        ClientProcessor blueE = new FakeClientProcessor(clientE, false, blue);

        delegate.add(blueA);
        delegate.add(greenA);
        delegate.add(redA);
        delegate.add(blueB);
        delegate.add(yellowB);
        delegate.add(greenC);
        delegate.add(yellowC);
        delegate.add(blackC);
        delegate.add(greenD);
        delegate.add(blueE);

        ClientProcessorsByComponent testSubject = new ClientProcessorsByComponent(delegate::iterator, "component");

        Iterator<ClientProcessor> iterator = testSubject.iterator();
        assertEquals(blueA, iterator.next());
        assertEquals(greenA, iterator.next());
        assertEquals(redA, iterator.next());
        assertEquals(blueB, iterator.next());
        assertEquals(yellowB, iterator.next());
        assertEquals(greenC, iterator.next());
        assertEquals(yellowC, iterator.next());
        assertEquals(greenD, iterator.next());
        assertEquals(blueE, iterator.next());
        assertFalse(iterator.hasNext());
    }
}