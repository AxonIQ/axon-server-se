package io.axoniq.axonserver.message.command;

import io.axoniq.axonserver.refactoring.messaging.api.Message;
import io.axoniq.axonserver.refactoring.messaging.command.CommandRegistrationCache;
import io.axoniq.axonserver.refactoring.messaging.command.api.Command;
import io.axoniq.axonserver.refactoring.messaging.command.api.CommandDefinition;
import io.axoniq.axonserver.refactoring.messaging.command.api.CommandHandler;
import org.junit.*;

import java.time.Instant;
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
        testSubject.add(commandHandler("client1", "command", 10));
        testSubject.add(commandHandler("client2", "command", 50));
        testSubject.add(commandHandler("client3", "command", 40));

        testSubject.add(commandHandler("client1", "anotherCommand", 4));
        testSubject.add(commandHandler("client2", "anotherCommand", 8));
        testSubject.add(commandHandler("client3", "anotherCommand", 16));


        for (int i = 0; i < 1000; i++) {
            testSubject.add(commandHandler(randomUUID().toString(), randomUUID().toString(), 44));
        }


        Map<String, AtomicInteger> counter = new HashMap<>();

        Command command = command("command", "context");
        for (int i = 0; i < 10000; i++) {
            String routingKey = randomUUID().toString();
            CommandHandler handler = testSubject.getHandlerForCommand(command, routingKey);
            counter.computeIfAbsent(handler.client().id(),
                                    c -> new AtomicInteger()).incrementAndGet();
        }

        assertTrue(matchPercentage("client1", 10, counter));
        assertTrue(matchPercentage("client2", 50, counter));
        assertTrue(matchPercentage("client3", 40, counter));
        System.out.println(counter);
    }

    private CommandHandler commandHandler(String client1, String command, int loadFactor) {
        return new DummyCommandHandler(command, client1, "someApplication", "someContext");
    }

    ;

    private Command command(String command, String context) {
        return new Command() {
            @Override
            public CommandDefinition definition() {
                return new CommandDefinition() {
                    @Override
                    public String name() {
                        return command;
                    }

                    @Override
                    public String context() {
                        return context;
                    }
                };
            }

            @Override
            public Message message() {
                return null;
            }

            @Override
            public String routingKey() {
                return null;
            }

            @Override
            public Instant timestamp() {
                return null;
            }
        };
    }

    @Test
    public void testLoadFactorSetToZero() {
        CommandRegistrationCache testSubject = new CommandRegistrationCache();

        testSubject.add(commandHandler("client1", "command", 0));
        testSubject.add(commandHandler("client2", "command", 200));

        Map<String, AtomicInteger> counter = new HashMap<>();

        Command command = command("command", "context");
        for (int i = 0; i < 10000; i++) {
            String routingKey = randomUUID().toString();
            CommandHandler handler = testSubject.getHandlerForCommand(command, routingKey);
            counter.computeIfAbsent(handler.client().id(),
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
}
