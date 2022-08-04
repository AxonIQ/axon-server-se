package io.axoniq.axonserver.commandprocesing.imp;

import io.axoniq.axonserver.commandprocessing.spi.Command;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandler;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandlerSubscription;
import io.axoniq.axonserver.commandprocessing.spi.CommandResult;
import io.axoniq.axonserver.commandprocessing.spi.Metadata;
import io.axoniq.axonserver.commandprocessing.spi.Payload;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;

public class MetaDataBasedHandlerSelectorTest {

    private final MetaDataBasedHandlerSelector testSubject = new MetaDataBasedHandlerSelector();

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void select() {
        Command command = command(Map.of("country", "NL"));

        CommandHandlerSubscription subscription1 = commandHandlerSubscription("target1",
                Map.of("region", "Europe",
                        "priority", 100,
                        "country", "NL"));
        CommandHandlerSubscription subscription2 = commandHandlerSubscription("target2",
                Map.of("region", "Europe",
                        "priority", 10,
                        "country", "IT"));
        Flux<CommandHandlerSubscription> candidates = Flux.just(subscription1, subscription2);
        Flux<CommandHandlerSubscription> result = testSubject.select(
                candidates,
                command);

        StepVerifier.create(result)
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    public void selectMultipleTargets() {
        Command command = command(Map.of("region", "Europe"));

        CommandHandlerSubscription subscription1 = commandHandlerSubscription("target1",
                Map.of("region", "Europe",
                        "priority", 100,
                        "country", "NL"));
        CommandHandlerSubscription subscription2 = commandHandlerSubscription("target2",
                Map.of("region", "Europe",
                        "priority", 10,
                        "country", "IT"));
        Flux<CommandHandlerSubscription> candidates = Flux.just(subscription2, subscription1);
        Flux<CommandHandlerSubscription> result = testSubject.select(
                candidates,
                command);

        StepVerifier.create(result)
                .expectNextCount(2)
                .verifyComplete();

    }

    @Test
    public void selectMultipleKeys() {
        Command command = command(Map.of("region", "Europe", "priority", "100"));

        CommandHandlerSubscription subscription1 = commandHandlerSubscription("target1",
                                                                              Map.of("region", "Europe",
                                                                                     "priority", 100,
                                                                                     "country", "NL"));
        CommandHandlerSubscription subscription2 = commandHandlerSubscription("target2",
                                                                              Map.of("region", "Europe",
                                                                                     "priority", 10,
                                                                                     "country", "IT"));
        Flux<CommandHandlerSubscription> candidates = Flux.just(subscription1, subscription2);
        Flux<CommandHandlerSubscription> result = testSubject.select(
                candidates,
                command);

        StepVerifier.create(result)
                .expectNextCount(1)
                .verifyComplete();
    }

    @Test
    public void selectNoMetadata() {
        Command command = command(Map.of());

        CommandHandlerSubscription subscription1 = commandHandlerSubscription("target1",
                                                                              Map.of("region", "Europe",
                                                                                     "priority", 100,
                                                                                     "country", "NL"));
        CommandHandlerSubscription subscription2 = commandHandlerSubscription("target2",
                                                                              Map.of("region", "Europe",
                                                                                     "priority", 10,
                                                                                     "country", "IT"));
        Flux<CommandHandlerSubscription> candidates = Flux.just(subscription1, subscription2);
        Flux<CommandHandlerSubscription> result = testSubject.select(
                candidates,
                command);

        StepVerifier.create(result)
                .expectNextCount(2)
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
                return MetaDataBasedHandlerSelectorTest.metadata(metadata);
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
                        return null;
                    }

                    @Override
                    public String context() {
                        return null;
                    }

                    @Override
                    public Metadata metadata() {
                        return MetaDataBasedHandlerSelectorTest.metadata(metadata);
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
            public Flux<String> metadataKeys() {
                return Flux.fromIterable(metadata.keySet());
            }

            @Override
            public <R extends Serializable> Optional<R> metadataValue(String metadataKey) {
                return Optional.ofNullable((R) metadata.get(metadataKey));
            }
        };
    }
}