package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.applicationevents.SubscriptionEvents.SubscribeCommand;
import io.axoniq.axonserver.grpc.SerializedCommand;
import io.axoniq.axonserver.grpc.SerializedCommandProviderInbound;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandSubscription;
import io.axoniq.axonserver.message.ClientIdentification;
import io.axoniq.axonserver.util.FakeStreamObserver;
import org.junit.*;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.TestCase.assertTrue;

/**
 * Unit tests for {@link CommandRegistrationCache} in relation with load factor.
 *
 * @author Sara Pellegrini
 */
public class CommandRegistrationCacheLoadFactorTest {

    @Test
    public void loadFactorBasedDistribution() {
        CommandRegistrationCache testSubject = new CommandRegistrationCache();

        testSubject.on(createSubscribeCommand("client1", 10));
        testSubject.on(createSubscribeCommand("client2", 50));
        testSubject.on(createSubscribeCommand("client3", 40));

        Map<String, AtomicInteger> counter = new HashMap<>();

        Command command = Command.newBuilder().setName("command").build();
        for (int i = 0; i < 100000; i++) {
            String routingKey = UUID.randomUUID().toString();
            CommandHandler handler = testSubject.getHandlerForCommand("context", command, routingKey);
            counter.computeIfAbsent(handler.getClient().getClient(), c -> new AtomicInteger()).incrementAndGet();
        }

        assertTrue(aroundPercentage("client1", 10, counter));
        assertTrue(aroundPercentage("client2", 50, counter));
        assertTrue(aroundPercentage("client3", 40, counter));
        System.out.println(counter);
    }


    private boolean aroundPercentage(String client, int expectedPercentage, Map<String, AtomicInteger> counter) {
        Integer total = counter.values().stream().map(AtomicInteger::get).reduce(Integer::sum).orElse(0);
        int actualPercentage = counter.get(client).get() * 100 / total;
        System.out.println(actualPercentage);
        return Math.abs(actualPercentage - expectedPercentage) < 5;
    }


    private SubscribeCommand createSubscribeCommand(
            String client, int loadFactor) {
        return new SubscribeCommand(
                "context",
                CommandSubscription.newBuilder()
                                   .setClientId(client)
                                   .setComponentName("componentName")
                                   .setCommand("command")
                                   .setLoadFactor(loadFactor)
                                   .build(),
                new FakeCommandHandler(client));
    }


    private static class FakeCommandHandler extends CommandHandler<SerializedCommandProviderInbound> {

        public FakeCommandHandler(String clientId) {
            super(new FakeStreamObserver<>(),
                  new ClientIdentification("context", clientId),
                  "componentName");
        }

        @Override
        public void dispatch(SerializedCommand request) {

        }

        @Override
        public void confirm(String messageId) {

        }
    }
}
