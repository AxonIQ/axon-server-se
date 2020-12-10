package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.applicationevents.SubscriptionEvents.SubscribeCommand;
import io.axoniq.axonserver.grpc.SerializedCommand;
import io.axoniq.axonserver.grpc.SerializedCommandProviderInbound;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandSubscription;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.test.FakeStreamObserver;
import org.junit.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.UUID.randomUUID;
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

        testSubject.on(createSubscribeCommand("client1", "command", 10));
        testSubject.on(createSubscribeCommand("client2", "command", 50));
        testSubject.on(createSubscribeCommand("client3", "command", 40));

        testSubject.on(createSubscribeCommand("client1", "anotherCommand", 4));
        testSubject.on(createSubscribeCommand("client2", "anotherCommand", 8));
        testSubject.on(createSubscribeCommand("client3", "anotherCommand", 16));

        for (int i = 0; i < 1000; i++) {
            testSubject.on(createSubscribeCommand(randomUUID().toString(), randomUUID().toString(), 44));
        }


        Map<String, AtomicInteger> counter = new HashMap<>();

        Command command = Command.newBuilder().setName("command").build();
        for (int i = 0; i < 10000; i++) {
            String routingKey = randomUUID().toString();
            CommandHandler handler = testSubject.getHandlerForCommand("context", command, routingKey);
            counter.computeIfAbsent(handler.getClientStreamIdentification().getClientStreamId(),
                                    c -> new AtomicInteger()).incrementAndGet();
        }

        assertTrue(matchPercentage("client1", 10, counter));
        assertTrue(matchPercentage("client2", 50, counter));
        assertTrue(matchPercentage("client3", 40, counter));
        System.out.println(counter);
    }

    @Test
    public void testLoadFactorSetToZero() {
        CommandRegistrationCache testSubject = new CommandRegistrationCache();

        testSubject.on(createSubscribeCommand("client1", "command", 0));
        testSubject.on(createSubscribeCommand("client2", "command", 200));

        Map<String, AtomicInteger> counter = new HashMap<>();

        Command command = Command.newBuilder().setName("command").build();
        for (int i = 0; i < 10000; i++) {
            String routingKey = randomUUID().toString();
            CommandHandler handler = testSubject.getHandlerForCommand("context", command, routingKey);
            counter.computeIfAbsent(handler.getClientStreamIdentification().getClientStreamId(),
                                    c -> new AtomicInteger()).incrementAndGet();
        }

        assertTrue(matchPercentage("client1", 33, counter));
        assertTrue(matchPercentage("client2", 66, counter));
        System.out.println(counter);
    }

    private boolean matchPercentage(String client, int expectedPercentage, Map<String, AtomicInteger> counter) {
        Integer total = counter.values().stream().map(AtomicInteger::get).reduce(Integer::sum).orElse(0);
        int actualPercentage = counter.get(client).get() * 100 / total;
        return Math.abs(actualPercentage - expectedPercentage) < 5;
    }

    private SubscribeCommand createSubscribeCommand(
            String client, String command, int loadFactor) {
        return new SubscribeCommand(
                "context",
                "clientStreamId", CommandSubscription.newBuilder()
                                                     .setClientId(client)
                                                     .setComponentName("componentName")
                                                     .setCommand(command)
                                                     .setLoadFactor(loadFactor)
                                                     .build(),
                new FakeCommandHandler(client));
    }


    private static class FakeCommandHandler extends CommandHandler<SerializedCommandProviderInbound> {

        public FakeCommandHandler(String clientId) {
            super(new FakeStreamObserver<>(),
                  new ClientStreamIdentification("context", clientId),
                  clientId, "componentName");
        }

        @Override
        public void dispatch(SerializedCommand request) {

        }

        @Override
        public void confirm(String messageId) {

        }
    }
}
