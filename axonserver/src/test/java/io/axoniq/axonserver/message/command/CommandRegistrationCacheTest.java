package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandProviderInbound;
import io.axoniq.axonserver.topology.Topology;
import io.axoniq.axonserver.util.CountingStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.*;

import static org.junit.Assert.*;

/**
 * Author: marc
 */
public class CommandRegistrationCacheTest {

    private CommandRegistrationCache registrationCache;
    private StreamObserver<CommandProviderInbound> streamObserver1;
    private StreamObserver<CommandProviderInbound> streamObserver2;

    @Before
    public void setup() {
        registrationCache = new CommandRegistrationCache();

        streamObserver1 = new CountingStreamObserver<>();
        streamObserver2 = new CountingStreamObserver<>();

        registrationCache.add(Topology.DEFAULT_CONTEXT, "command1", new DirectCommandHandler(streamObserver1, "client1", "component"));
        registrationCache.add(Topology.DEFAULT_CONTEXT,"command1", new DirectCommandHandler(streamObserver2, "client2", "component"));
        registrationCache.add(Topology.DEFAULT_CONTEXT,"command2", new DirectCommandHandler(streamObserver2, "client2", "component"));
    }

    @Test
    public void removeCommandSubscription() {
        registrationCache.remove(Topology.DEFAULT_CONTEXT,"command1", "client2");
        assertTrue(registrationCache.getAll().containsKey(new DirectCommandHandler(streamObserver2, "client2", "component")));
        assertEquals(1, registrationCache.getAll().get(new DirectCommandHandler(streamObserver2, "client2", "component")).size());
    }

    @Test
    public void removeLastCommandSubscription() {
        registrationCache.remove(Topology.DEFAULT_CONTEXT,"command1", "client1");
        assertFalse(registrationCache.getAll().containsKey(new DirectCommandHandler(streamObserver1, "client1", "component")));
    }

    @Test
    public void removeConnection() {
        registrationCache.remove("client2");
        assertFalse(registrationCache.getAll().containsKey(new DirectCommandHandler(streamObserver1, "client2", "component")));
    }

    @Test
    public void add() {
        registrationCache.add(Topology.DEFAULT_CONTEXT,"command2", new DirectCommandHandler(streamObserver1, "client1", "component"));
        assertEquals(2, registrationCache.getAll().get(new DirectCommandHandler(streamObserver1, "client1", "component")).size());
    }

    @Test
    public void get() {
        assertNotNull(registrationCache.getNode(Topology.DEFAULT_CONTEXT, Command.newBuilder().setName("command1").build(),
                "command1"));
    }

    @Test
    public void getNotFound() {
        assertNull(registrationCache.getNode(Topology.DEFAULT_CONTEXT, Command.newBuilder().setName("command3").build(),
                "command1"));
    }

    @Test
    public void findByExistingClient() {
        assertNotNull(registrationCache.findByClientAndCommand("client2", Command.newBuilder().setName("command1").build()));
    }

    @Test
    public void findByNonExistingClient() {
        assertNull(registrationCache.findByClientAndCommand("client9", Command.newBuilder().setName("command1").build()));
    }

}