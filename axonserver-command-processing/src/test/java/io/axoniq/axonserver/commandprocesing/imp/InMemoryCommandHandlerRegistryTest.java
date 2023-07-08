package io.axoniq.axonserver.commandprocesing.imp;

import io.axoniq.axonserver.commandprocessing.spi.Command;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandler;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandlerSubscription;
import io.axoniq.axonserver.commandprocessing.spi.CommandResult;
import io.axoniq.axonserver.commandprocessing.spi.Metadata;
import io.axoniq.axonserver.commandprocessing.spi.Payload;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static reactor.core.publisher.Mono.when;

/**
 * @author Stefan Dragisic
 */
public class InMemoryCommandHandlerRegistryTest {

    List<HandlerSelectorStrategy> handlerSelectorStrategyList = List.of(new MetaDataBasedHandlerSelectorStrategy());

    private final InMemoryCommandHandlerRegistry testSubject = new InMemoryCommandHandlerRegistry(handlerSelectorStrategyList);

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void selectFromOne() {
        Command command = command(Map.of("country", "NL"));

        CommandHandlerSubscription commandHandlerSubscription = commandHandlerSubscription("target1",
                Map.of("region", "Europe",
                        "priority", 100,
                        "country", "NL"));
        Mono<Void> register = testSubject.register(commandHandlerSubscription);

        Mono<CommandHandlerSubscription> selectHandler = testSubject.handler(command);

        StepVerifier.create(register.then(selectHandler))
                .expectNextMatches(n->n.equals(commandHandlerSubscription))
                .verifyComplete();
    }

    @Test
    public void selectFromMultiple() {
        Command command = command(Map.of("country", "NL", "priority", "100"));

        CommandHandlerSubscription commandHandlerSubscription1 = commandHandlerSubscription("target1",
                Map.of("region", "Europe",
                        "priority", 10,
                        "country", "IT"));
        CommandHandlerSubscription commandHandlerSubscription2 = commandHandlerSubscription("target2",
                Map.of("region", "Europe",
                        "priority", 100,
                        "country", "NL"));
        Mono<Void> registerFirst = testSubject.register(commandHandlerSubscription1);
        Mono<Void> registerSecond = testSubject.register(commandHandlerSubscription2);

        Mono<CommandHandlerSubscription> selectHandler = testSubject.handler(command);

        StepVerifier.create(
                when(registerFirst)
                        .and(registerSecond)
                        .then(selectHandler)
                )
                .expectNextMatches(n->n.equals(commandHandlerSubscription2))
                .verifyComplete();
    }

    private Command command(Map<String, Serializable> metadata) {
        return new Command() {
            @Override
            public String id() {
                return null;
            }

            @Override
            public String commandName() {
                return "command";
            }

            @Override
            public String context() {
                return "context";
            }

            @Override
            public Payload payload() {
                return null;
            }

            @Override
            public Metadata metadata() {
                return InMemoryCommandHandlerRegistryTest.metadata(metadata);
            }
        };
    }

    private CommandHandlerSubscription commandHandlerSubscription(String id, Map<String, Serializable> metadata) {
        return new CommandHandlerSubscription() {
            @Override
            public CommandHandler commandHandler() {
                return new CommandHandler() {
                    @Override
                    public String id() {
                        return id;
                    }

                    @Override
                    public String description() {
                        return null;
                    }

                    @Override
                    public String commandName() {
                        return "command";
                    }

                    @Override
                    public String context() {
                        return "context";
                    }

                    @Override
                    public Metadata metadata() {
                        return InMemoryCommandHandlerRegistryTest.metadata(metadata);
                    }
                };
            }

            @Override
            public Mono<CommandResult> dispatch(Command command) {
                return null;
            }
        };
    }

    private static Metadata metadata(Map<String, Serializable> metadata) {
        return new Metadata() {
            @Override
            public Iterable<String> metadataKeys() {
                return metadata.keySet();
            }

            @Override
            public <R extends Serializable> Optional<R> metadataValue(String metadataKey) {
                return Optional.ofNullable((R) metadata.get(metadataKey));
            }
        };
    }
}