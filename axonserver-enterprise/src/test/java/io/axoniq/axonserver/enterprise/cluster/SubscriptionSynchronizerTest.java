package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.applicationevents.SubscriptionEvents.SubscribeCommand;
import io.axoniq.axonserver.applicationevents.SubscriptionEvents.UnsubscribeCommand;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents.AxonServerInstanceConnected;
import io.axoniq.axonserver.enterprise.cluster.internal.RemoteConnection;
import io.axoniq.axonserver.enterprise.cluster.internal.StubFactory;
import io.axoniq.axonserver.enterprise.config.ClusterConfiguration;
import io.axoniq.axonserver.grpc.command.CommandSubscription;
import io.axoniq.axonserver.message.ClientIdentification;
import io.axoniq.axonserver.message.command.CommandRegistrationCache;
import io.axoniq.axonserver.message.command.DirectCommandHandler;
import io.axoniq.axonserver.message.query.QueryRegistrationCache;
import org.junit.*;

import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author Sara Pellegrini
 */
public class SubscriptionSynchronizerTest {

    private final String myComponentName = "myComponentName";
    private final String myClientId = "myClientId";
    private final String myCommand = "myCommand";
    private final String myContext = "myContext";

    private final ClientIdentification clientIdentification = new ClientIdentification(myContext, myClientId);
    private final CommandRegistrationCache commandRegistrationCache = new CommandRegistrationCache();
    private final QueryRegistrationCache queryRegistrationCache = new QueryRegistrationCache((definition, component, handlers) -> clientIdentification);
    private final List<RemoteConnection> activeConnections = new LinkedList<>();
    private final SubscriptionSynchronizer testSubject = new SubscriptionSynchronizer(commandRegistrationCache,
                                                                                      queryRegistrationCache,
                                                                                      activeConnections::stream,
                                                                                      s -> System.out.println(
                                                                                              "Connection Closed"));


    @Before
    public void setUp() throws Exception {
        activeConnections.clear();
        activeConnections.add(new FakeRemoteConnection());
    }

    @Test
    public void testSubscriptionIsPropagatedToActiveConnections() {
        SubscribeCommand event = subscribeCommand();
        testSubject.on(event);
        activeConnections.forEach(connection -> {
            FakeRemoteConnection fakeRemoteConnection = (FakeRemoteConnection) connection;
            assertEquals(1, fakeRemoteConnection.commandSubscriptionsPropagated.size());
            assertEquals(event.getRequest(), fakeRemoteConnection.commandSubscriptionsPropagated.get(0));
        });
    }

    @Test
    public void testSubscriptionIsPropagatedToNewAxonServerNodes() {
        SubscribeCommand event = subscribeCommand();
        commandRegistrationCache.on(event);

        FakeRemoteConnection fakeRemoteConnection = new FakeRemoteConnection();
        testSubject.on(new AxonServerInstanceConnected(fakeRemoteConnection));
        assertEquals(1, fakeRemoteConnection.commandSubscriptionsPropagated.size());
        assertEquals(event.getRequest(), fakeRemoteConnection.commandSubscriptionsPropagated.get(0));
    }

    @Test
    public void testUnsubscriptionIsPropagatedToActiveConnections() {
        UnsubscribeCommand event = unsubscribeCommand();
        testSubject.on(event);
        activeConnections.forEach(connection -> {
            FakeRemoteConnection fakeRemoteConnection = (FakeRemoteConnection) connection;
            assertEquals(1, fakeRemoteConnection.commandUnSubscriptionsPropagated.size());
            assertEquals(event.getRequest(), fakeRemoteConnection.commandUnSubscriptionsPropagated.get(0));
        });
    }


    private UnsubscribeCommand unsubscribeCommand() {
        return new UnsubscribeCommand("context", commandSubscription(), false);
    }

    private SubscribeCommand subscribeCommand() {

        return new SubscribeCommand(myContext,
                                    commandSubscription(),
                                    new DirectCommandHandler(null, clientIdentification, myComponentName));
    }

    private CommandSubscription commandSubscription() {
        return CommandSubscription.newBuilder()
                                  .setClientId(myClientId)
                                  .setComponentName(myComponentName)
                                  .setLoadFactor(30)
                                  .setCommand(myCommand)
                                  .build();
    }

    private final static class FakeRemoteConnection extends RemoteConnection {

        private final List<CommandSubscription> commandSubscriptionsPropagated = new LinkedList<>();
        private final List<CommandSubscription> commandUnSubscriptionsPropagated = new LinkedList<>();

        public FakeRemoteConnection() {
            super(new FakeClusterController(), null, new StubFactory() {
                  },
                  null, null, (h, i, t) -> true);
        }

        @Override
        public void unsubscribeCommand(String context, CommandSubscription unsubscribeRequest) {
            commandUnSubscriptionsPropagated.add(unsubscribeRequest);
        }

        @Override
        public void subscribeCommand(String context, CommandSubscription commandSubscription) {
            commandSubscriptionsPropagated.add(commandSubscription);
        }
    }

    private static class FakeClusterController extends ClusterController {

        public FakeClusterController() {
            super(null,
                  new ClusterConfiguration(),
                  null,
                  null,
                  null,
                  null,
                  null,
                  null,
                  null,
                  null,
                  null);
        }
    }
}