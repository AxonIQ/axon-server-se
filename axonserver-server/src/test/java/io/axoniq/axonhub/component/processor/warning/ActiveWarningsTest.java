package io.axoniq.axonhub.component.processor.warning;

import org.junit.*;

import java.util.Iterator;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * Created by Sara Pellegrini on 23/03/2018.
 * sara.pellegrini@gmail.com
 */
public class ActiveWarningsTest {

    @Test
    public void testOne() {
        Warning activeWarning = new FakeWarning(true, "Active warning");
        List<Warning> fakes = asList(new FakeWarning(false, "Inactive warning"),
                                     activeWarning,
                                     new FakeWarning(false, "Other inactive warning")
        );

        Iterator<Warning> iterator = new ActiveWarnings(fakes).iterator();
        assertTrue(iterator.hasNext());
        assertEquals(activeWarning,iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testNone() {
        List<Warning> fakes = asList(new FakeWarning(false, "Inactive warning"),
                                     new FakeWarning(false, "Other inactive warning")
        );

        Iterator<Warning> iterator = new ActiveWarnings(fakes).iterator();
        assertFalse(iterator.hasNext());
    }
}