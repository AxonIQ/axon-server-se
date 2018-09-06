package io.axoniq.axonhub.component;

import io.axoniq.axonhub.context.ContextController;
import org.junit.*;

import java.util.Iterator;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * Created by Sara Pellegrini on 21/03/2018.
 * sara.pellegrini@gmail.com
 */
public class ComponentItemsTest {

    @Test
    public void testSomeMatch(){
        FakeComponentItem matching = new FakeComponentItem(true);
        List<FakeComponentItem> fakes = asList(new FakeComponentItem(false), matching, new FakeComponentItem(false));
        ComponentItems<FakeComponentItem> component = new ComponentItems<>("component", ContextController.DEFAULT, fakes);
        Iterator<FakeComponentItem> iterator = component.iterator();
        assertTrue(iterator.hasNext());
        assertEquals(matching, iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testNoMatch(){
        List<FakeComponentItem> fakes = asList(new FakeComponentItem(false), new FakeComponentItem(false));
        ComponentItems<FakeComponentItem> component = new ComponentItems<>("component", ContextController.DEFAULT, fakes);
        Iterator<FakeComponentItem> iterator = component.iterator();
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testAllMatch(){
        FakeComponentItem matching = new FakeComponentItem(true);
        List<FakeComponentItem> fakes = asList(matching, matching);
        ComponentItems<FakeComponentItem> component = new ComponentItems<>("component", ContextController.DEFAULT, fakes);
        Iterator<FakeComponentItem> iterator = component.iterator();
        assertTrue(iterator.hasNext());
        assertEquals(matching, iterator.next());
        assertEquals(matching, iterator.next());
        assertFalse(iterator.hasNext());
    }

}