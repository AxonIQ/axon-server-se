package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.TestSystemInfoProvider;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.cluster.internal.ProxyCommandHandler;
import io.axoniq.axonserver.message.ClientIdentification;
import io.axoniq.axonserver.message.command.CommandRegistrationCache;
import io.axoniq.axonserver.message.command.DirectCommandHandler;
import io.axoniq.axonserver.message.query.QueryHandlerSelector;
import io.axoniq.axonserver.message.query.QueryRegistrationCache;
import io.axoniq.axonserver.topology.Topology;
import io.axoniq.axonserver.util.CountingStreamObserver;
import org.junit.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class SubscriptionCountBasedNodeSelectionStrategyTest {

    private static final String ME = "THIS_NODE";
    private SubscriptionCountBasedNodeSelectionStrategy testSubject;
    private CommandRegistrationCache commandRegistry;
    @Before
    public void setUp() {
        QueryHandlerSelector queryHandlerSelector= (queryDefinition, componentName, queryHandlers) -> null;
        QueryRegistrationCache queryRegistry = new QueryRegistrationCache(queryHandlerSelector);
        commandRegistry = new CommandRegistrationCache();
        MessagingPlatformConfiguration configuration = new MessagingPlatformConfiguration(new TestSystemInfoProvider());
        configuration.setName(ME);
        testSubject = new SubscriptionCountBasedNodeSelectionStrategy(commandRegistry, queryRegistry, configuration);
    }

    @Test
    public void selectNodeNoSubscriptions() {
        Collection<String> activeNodes = Collections.emptyList();
        assertEquals(ME, testSubject.selectNode(new ClientIdentification(Topology.DEFAULT_CONTEXT,"client1"), "component1", activeNodes));
    }

    @Test
    public void selectNodeWithComponent() {
        Collection<String> activeNodes = Arrays.asList(ME, "server1");
        commandRegistry.add("command1",
                            new DirectCommandHandler(new CountingStreamObserver<>(), new ClientIdentification(Topology.DEFAULT_CONTEXT,
                                                     "client1"), "component1"));
        commandRegistry.add("command1",
                new ProxyCommandHandler(new CountingStreamObserver<>(), new ClientIdentification(Topology.DEFAULT_CONTEXT, "client2"), "component2",  "server1"));

        assertEquals(ME,testSubject.selectNode(new ClientIdentification(Topology.DEFAULT_CONTEXT,"client3"), "component2", activeNodes));
        assertEquals("server1", testSubject.selectNode(new ClientIdentification(Topology.DEFAULT_CONTEXT,"client3"), "component1", activeNodes) );
    }
    @Test
    public void selectNodeWithoutSubscriptions() {
        Collection<String> activeNodes = Arrays.asList(ME, "server1", "server2");
        commandRegistry.add("command1",
                new DirectCommandHandler(new CountingStreamObserver<>(), new ClientIdentification(Topology.DEFAULT_CONTEXT, "client1"), "component1"));
        commandRegistry.add("command1",
                new ProxyCommandHandler(new CountingStreamObserver<>(), new ClientIdentification(Topology.DEFAULT_CONTEXT, "client2"), "component2", "server1"));

        assertEquals("server2",testSubject.selectNode(new ClientIdentification(Topology.DEFAULT_CONTEXT,"client3"), "component3", activeNodes) );
    }

}
