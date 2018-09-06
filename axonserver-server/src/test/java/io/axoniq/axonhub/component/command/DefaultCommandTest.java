package io.axoniq.axonhub.component.command;

import com.google.common.collect.ImmutableSet;
import io.axoniq.axonhub.context.ContextController;
import io.axoniq.axonhub.message.command.CommandHandler;
import io.axoniq.axonhub.message.command.CommandRegistrationCache;
import io.axoniq.axonhub.message.command.DirectCommandHandler;
import io.axoniq.axonhub.serializer.GsonMedia;
import org.junit.*;

import static org.junit.Assert.*;

/**
 * Created by Sara Pellegrini on 23/03/2018.
 * sara.pellegrini@gmail.com
 */
public class DefaultCommandTest {

    private DefaultCommand defaultCommand;

    @Before
    public void setUp() throws Exception {
        ImmutableSet<CommandHandler> commandHandlers = ImmutableSet.of(new DirectCommandHandler(null, "client", "componentA"));
        defaultCommand = new DefaultCommand(new CommandRegistrationCache.RegistrationEntry(ContextController.DEFAULT,
                                                                                           "commandName"), commandHandlers);
    }

    @Test
    public void belongsToComponent() {
        assertTrue(defaultCommand.belongsToComponent("componentA"));
    }

    @Test
    public void notBelongsToComponent() {
        assertFalse(defaultCommand.belongsToComponent("componentB"));
    }

    @Test
    public void printOn() {
        GsonMedia gsonMedia = new GsonMedia();
        defaultCommand.printOn(gsonMedia);
        assertEquals("{\"name\":\"commandName\"}", gsonMedia.toString());
    }
}