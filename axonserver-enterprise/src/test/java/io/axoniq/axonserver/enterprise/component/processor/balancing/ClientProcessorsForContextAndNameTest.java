package io.axoniq.axonserver.enterprise.component.processor.balancing;

import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import org.junit.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import static org.junit.Assert.*;

/**
 * Test class for the {@link ClientProcessorsForContextAndName} class.
 *
 * @author Steven van Beelen
 */
public class ClientProcessorsForContextAndNameTest {

    private static final String CLIENT_A = "clientA";
    private static final String CLIENT_B = "clientB";
    private static final String CLIENT_C = "clientC";

    private static final boolean BELONGS_TO_CONTEXT = true;
    private static final boolean DOES_NOT_BELONG_TO_CONTEXT = false;

    private static final String BLUE_PROCESSOR = "Blue";
    private static final String GREEN_PROCESSOR = "Green";
    private static final String RED_PROCESSOR = "Red";

    @Test
    public void testIteratorReturnsMatchingProcessors() {
        Collection<ClientProcessor> testProcessors = new ArrayList<>();

        ClientProcessor blueA = new FakeClientProcessor(CLIENT_A, BELONGS_TO_CONTEXT, BLUE_PROCESSOR);
        ClientProcessor greenA = new FakeClientProcessor(CLIENT_A, BELONGS_TO_CONTEXT, GREEN_PROCESSOR);
        ClientProcessor redA = new FakeClientProcessor(CLIENT_A, BELONGS_TO_CONTEXT, RED_PROCESSOR);
        testProcessors.add(blueA);
        testProcessors.add(greenA);
        testProcessors.add(redA);

        ClientProcessor blueB = new FakeClientProcessor(CLIENT_B, BELONGS_TO_CONTEXT, BLUE_PROCESSOR);
        ClientProcessor greenB = new FakeClientProcessor(CLIENT_B, BELONGS_TO_CONTEXT, GREEN_PROCESSOR);
        ClientProcessor redB = new FakeClientProcessor(CLIENT_B, BELONGS_TO_CONTEXT, RED_PROCESSOR);
        testProcessors.add(blueB);
        testProcessors.add(greenB);
        testProcessors.add(redB);

        ClientProcessor blueC = new FakeClientProcessor(CLIENT_C, DOES_NOT_BELONG_TO_CONTEXT, BLUE_PROCESSOR);
        ClientProcessor greenC = new FakeClientProcessor(CLIENT_C, DOES_NOT_BELONG_TO_CONTEXT, GREEN_PROCESSOR);
        ClientProcessor redC = new FakeClientProcessor(CLIENT_C, DOES_NOT_BELONG_TO_CONTEXT, RED_PROCESSOR);
        testProcessors.add(blueC);
        testProcessors.add(greenC);
        testProcessors.add(redC);

        ClientProcessorsForContextAndName testSubject =
                new ClientProcessorsForContextAndName(testProcessors::iterator, "some-context", BLUE_PROCESSOR);

        Iterator<ClientProcessor> resultIterator = testSubject.iterator();
        assertEquals(blueA, resultIterator.next());
        assertEquals(blueB, resultIterator.next());
        assertFalse(resultIterator.hasNext());
    }
}