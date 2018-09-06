package io.axoniq.axonhub.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.axoniq.axonhub.component.command.ComponentCommand;
import io.axoniq.axonhub.context.ContextController;
import io.axoniq.axonhub.message.command.CommandDispatcher;
import io.axoniq.axonhub.message.command.CommandRegistrationCache;
import io.axoniq.axonhub.message.command.DirectCommandHandler;
import io.axoniq.axonhub.serializer.GsonMedia;
import io.axoniq.axonhub.util.CountingStreamObserver;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.runners.*;

import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Author: marc
 */
@RunWith(MockitoJUnitRunner.class)
public class CommandRestControllerTest {
    private CommandRestController testSubject;
    @Mock
    private CommandDispatcher commandDispatcher;

    @Before
    public void setUp() {
        CommandRegistrationCache commandRegistationCache = new CommandRegistrationCache();
        commandRegistationCache.add(ContextController.DEFAULT, "DoIt", new DirectCommandHandler(new CountingStreamObserver<>(), "client", "component"));
        testSubject = new CommandRestController(commandDispatcher, commandRegistationCache);
    }

    @Test
    public void get() throws Exception {
        List<CommandRestController.JsonClientMapping> commands = testSubject.get();
        ObjectMapper mapper = new ObjectMapper();
        assertNotEquals("[]", mapper.writeValueAsString(commands));
    }

    @Test
    public void getByComponent(){
        Iterator<ComponentCommand> iterator = testSubject.getByComponent("component", ContextController.DEFAULT).iterator();
        assertTrue(iterator.hasNext());
        GsonMedia gsonMedia = new GsonMedia();
        iterator.next().printOn(gsonMedia);
        assertEquals("{\"name\":\"DoIt\"}", gsonMedia.toString());
        assertFalse(iterator.hasNext());

    }

    @Test
    public void getByNotExistingComponent(){
        Iterator<ComponentCommand> iterator = testSubject.getByComponent("otherComponent", ContextController.DEFAULT).iterator();
        assertFalse(iterator.hasNext());
    }


}