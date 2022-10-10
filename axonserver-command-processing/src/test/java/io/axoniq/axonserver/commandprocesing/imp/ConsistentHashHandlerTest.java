package io.axoniq.axonserver.commandprocesing.imp;

import io.axoniq.axonserver.commandprocessing.spi.Command;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandler;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandlerSubscription;
import io.axoniq.axonserver.commandprocessing.spi.CommandResult;
import io.axoniq.axonserver.commandprocessing.spi.Metadata;
import io.axoniq.axonserver.commandprocessing.spi.Payload;
import org.junit.Test;
import reactor.core.publisher.Mono;

import java.io.Serializable;
import java.util.Optional;
import java.util.Set;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;

public class ConsistentHashHandlerTest {

    private final ConsistentHashHandlerStrategy consistentHashHandler = new ConsistentHashHandlerStrategy(commandHandler -> Optional.empty(),
                                                                                          command -> Optional.of(
                                                                                                  "routingKey1"));

    @Test
    public void select() {
        CommandHandler commandHandler1 = commandHandler("handler1");
        CommandHandler commandHandler2 = commandHandler("handler2");
        consistentHashHandler.onCommandHandlerSubscribed(commandHandler1).block();
        consistentHashHandler.onCommandHandlerSubscribed(commandHandler2).block();

        Set<CommandHandlerSubscription> handlers = consistentHashHandler.select(Set.of(commandHandlerSubscription(
                                                                                               commandHandler1),
                                                                                       commandHandlerSubscription(
                                                                                               commandHandler2)),
                                                                                command("routingKey1"));
        assertEquals(1, handlers.size());
    }

    private Command command(String routingKey1) {
        return new Command() {
            @Override
            public String id() {
                return null;
            }

            @Override
            public String commandName() {
                return "Command";
            }

            @Override
            public String context() {
                return "Context";
            }

            @Override
            public Payload payload() {
                return null;
            }

            @Override
            public Metadata metadata() {
                return new Metadata() {
                    @Override
                    public Iterable<String> metadataKeys() {
                        return null;
                    }

                    @Override
                    public <R extends Serializable> Optional<R> metadataValue(String metadataKey) {
                        if (Command.ROUTING_KEY.equals(metadataKey)) {
                            return Optional.of((R) routingKey1);
                        }
                        return Optional.empty();
                    }
                };
            }
        };
    }

    private CommandHandlerSubscription commandHandlerSubscription(CommandHandler commandHandler1) {
        return new CommandHandlerSubscription() {
            @Override
            public CommandHandler commandHandler() {
                return commandHandler1;
            }

            @Override
            public Mono<CommandResult> dispatch(Command command) {
                return null;
            }
        };
    }

    private CommandHandler commandHandler(String handler) {
        return new CommandHandler() {
            @Override
            public String id() {
                return handler;
            }

            @Override
            public String description() {
                return null;
            }

            @Override
            public String commandName() {
                return "Command";
            }

            @Override
            public String context() {
                return "Context";
            }

            @Override
            public Metadata metadata() {
                return new Metadata() {
                    @Override
                    public Iterable<String> metadataKeys() {
                        return emptyList();
                    }

                    @Override
                    public <R extends Serializable> Optional<R> metadataValue(String metadataKey) {
                        return Optional.empty();
                    }
                };
            }
        };
    }
}