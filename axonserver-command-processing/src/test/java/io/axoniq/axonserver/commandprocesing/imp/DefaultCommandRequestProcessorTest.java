package io.axoniq.axonserver.commandprocesing.imp;

import io.axoniq.axonserver.commandprocessing.spi.Command;
import io.axoniq.axonserver.commandprocessing.spi.CommandException;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandler;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandlerSubscription;
import io.axoniq.axonserver.commandprocessing.spi.CommandRequest;
import io.axoniq.axonserver.commandprocessing.spi.CommandResult;
import io.axoniq.axonserver.commandprocessing.spi.Metadata;
import io.axoniq.axonserver.commandprocessing.spi.Payload;
import io.axoniq.axonserver.commandprocessing.spi.Registration;
import io.axoniq.axonserver.commandprocessing.spi.ResultPayload;
import io.axoniq.axonserver.commandprocessing.spi.interceptor.CommandFailedInterceptor;
import io.axoniq.axonserver.commandprocessing.spi.interceptor.CommandHandlerSubscribedInterceptor;
import io.axoniq.axonserver.commandprocessing.spi.interceptor.CommandHandlerUnsubscribedInterceptor;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

public class DefaultCommandRequestProcessorTest {

    public static final String COMMAND_NAME = "doIt";
    public static final String CONTEXT = "sample";
    public static final int LOWER_PRIORITY = 10;
    private final DefaultCommandRequestProcessor testSubject = new DefaultCommandRequestProcessor(Collections.emptyList());

    private final CommandHandlerSubscription handler = new CommandHandlerSubscription() {

        private final CommandHandler grpcCommandHandler = new GrpcCommandHandler(COMMAND_NAME, CONTEXT, Map.of());


        @Override
        public CommandHandler commandHandler() {
            return grpcCommandHandler;
        }

        @Override
        public Mono<CommandResult> dispatch(Command command) {
            return Mono.just(new CommandResult() {

                private final String id = UUID.randomUUID().toString();

                @Override
                public String id() {
                    return id;
                }

                @Override
                public String commandId() {
                    return command.id();
                }

                @Override
                public ResultPayload payload() {
                    return new ResultPayload() {
                        private final Payload wrapped = DefaultCommandRequestProcessorTest.payload("hello, world");

                        @Override
                        public boolean error() {
                            return false;
                        }

                        @Override
                        public String type() {
                            return wrapped.type();
                        }

                        @Override
                        public String contentType() {
                            return wrapped.contentType();
                        }

                        @Override
                        public Flux<Byte> data() {
                            return wrapped.data();
                        }
                    };
                }

                @Override
                public Metadata metadata() {
                    return command.metadata();
                }
            });
        }
    };


    @Test
    public void dispatch() {
        AtomicReference<CommandResult> result = new AtomicReference<>();
        testSubject.register(handler)
                   .then(testSubject.dispatch(new CommandRequest() {
                       @Override
                       public Command command() {
                           return new GrpcCommand(COMMAND_NAME, CONTEXT, payload("Request payload"), Map.of());
                       }

                       @Override
                       public Mono<Void> complete() {
                           result.set(null);
                           return Mono.empty();
                       }

                       @Override
                       public Mono<Void> complete(CommandResult r) {
                           result.set(r);
                           return Mono.empty();
                       }

                       @Override
                       public Mono<Void> completeExceptionally(Throwable t) {
                           t.printStackTrace();
                           return Mono.empty();
                       }
                   }))
                   .then(testSubject.unregister(handler.commandHandler().id()))
                   .block();

        assertNotNull(result.get());
    }

    @Test
    public void dispatchNoHandler() {
        StepVerifier.create(testSubject.dispatch(new CommandRequest() {
            @Override
            public Command command() {
                return new GrpcCommand(COMMAND_NAME, "Other context", payload("Request payload"), Map.of());
            }

            @Override
            public Mono<Void> complete() {
                return Mono.empty();
            }

            @Override
            public Mono<Void> complete(CommandResult r) {
                return Mono.empty();
            }

            @Override
            public Mono<Void> completeExceptionally(Throwable t) {
                return Mono.empty();
            }
        })).expectError(NoHandlerFoundException.class).verify();
    }

    @Test
    public void dispatchReturnEmpty() {

        AtomicReference<CommandResult> result = new AtomicReference<>();

        CommandHandlerSubscription handler2 = new CommandHandlerSubscription() {
            @Override
            public CommandHandler commandHandler() {
                return new GrpcCommandHandler(COMMAND_NAME, CONTEXT, Map.of());
            }

            @Override
            public Mono<CommandResult> dispatch(Command command) {
                return Mono.empty();
            }
        };
        testSubject.register(handler2)
                   .then(testSubject.dispatch(new CommandRequest() {
                       @Override
                       public Command command() {
                           return new GrpcCommand(COMMAND_NAME, CONTEXT, payload("Request payload"), Map.of());
                       }

                       @Override
                       public Mono<Void> complete() {
                           result.set(null);
                           return Mono.empty();
                       }

                       @Override
                       public Mono<Void> complete(CommandResult r) {
                           result.set(r);
                           return Mono.empty();
                       }

                       @Override
                       public Mono<Void> completeExceptionally(Throwable t) {
                           t.printStackTrace();
                           return Mono.empty();
                       }
                   }))
                   .then(testSubject.unregister(handler.commandHandler().id()))
                   .block();

        assertNull(result.get());
    }

    @Test
    public void registerInterceptor() {
        List<String> actions = new ArrayList<>();
        Registration registration1 = testSubject.registerInterceptor(
                CommandHandlerSubscribedInterceptor.class,
                new CommandHandlerSubscribedInterceptor() {
                    @Override
                    public Mono<Void> onCommandHandlerSubscribed(
                            CommandHandler commandHandler) {
                        actions.add("first");
                        return Mono.empty();
                    }

                    @Override
                    public int priority() {
                        return LOWER_PRIORITY;
                    }
                });
        Registration registration2 = testSubject.registerInterceptor(CommandHandlerSubscribedInterceptor.class,
                                                                     commandHandler -> {
                                                                         actions.add("second");
                                                                         return Mono.empty();
                                                                     });
        testSubject.register(handler).block();
        assertEquals(List.of("second", "first"), actions);
        registration1.cancel().block();
        registration2.cancel().block();
    }

    @Test
    public void interceptorsExecuted() {
        SimpleHandlerSelector simpleHandlerSelector = new SimpleHandlerSelector();
        DefaultCommandRequestProcessor testSubject2 = new DefaultCommandRequestProcessor(List.of(simpleHandlerSelector));
        testSubject2.registerInterceptor(CommandHandlerSubscribedInterceptor.class, simpleHandlerSelector);
        testSubject2.registerInterceptor(CommandHandlerUnsubscribedInterceptor.class, simpleHandlerSelector);
        testSubject2.register(handler).block();
        assertTrue(simpleHandlerSelector.subscribedInterceptorCalled());
        testSubject2.unregister(handler.commandHandler().id()).block();
        assertTrue(simpleHandlerSelector.unsubscribedInterceptorCalled());
    }

    @Test
    public void dispatchReturnError() {
        AtomicReference<CommandResult> result = new AtomicReference<>();
        List<CommandException> commandExceptions = new LinkedList<>();
        testSubject.registerInterceptor(CommandFailedInterceptor.class,
                                        commandException -> commandException.doOnNext(commandExceptions::add));
        testSubject.register(handler)
                   .then(testSubject.dispatch(new CommandRequest() {
                       @Override
                       public Command command() {
                           return new GrpcCommand(COMMAND_NAME, "anotherContext", payload("String"), Map.of());
                       }

                       @Override
                       public Mono<Void> complete() {
                           result.set(null);
                           return Mono.empty();
                       }

                       @Override
                       public Mono<Void> complete(CommandResult r) {
                           result.set(r);
                           return Mono.empty();
                       }

                       @Override
                       public Mono<Void> completeExceptionally(Throwable t) {
                           return Mono.error(t);
                       }
                   }))
                   .then(testSubject.unregister(handler.commandHandler().id()))
                   .onErrorResume(e -> Mono.empty())
                   .block();

        assertNull(result.get());
        assertEquals(1, commandExceptions.size());
    }

    private static Payload payload(String string) {
        return new Payload() {
            @Override
            public String type() {
                return null;
            }

            @Override
            public String contentType() {
                return "String";
            }

            @Override
            public Flux<Byte> data() {
                return Flux.create(sink -> {
                    byte[] bytes = string.getBytes();
                    for (byte aByte : bytes) {
                        sink.next(aByte);
                    }
                    sink.complete();
                });
            }
        };
    }

    private static class GrpcCommand implements Command {

        private final String command;
        private final String context;
        private final Payload payload;
        private final Metadata metadata;

        private final String id = UUID.randomUUID().toString();

        public GrpcCommand(String command, String context, Payload payload, Map<String, Serializable> metadata) {
            this.command = command;
            this.context = context;
            this.payload = payload;
            this.metadata = DefaultCommandRequestProcessorTest.metadata(metadata);
        }

        @Override
        public String id() {
            return id;
        }

        @Override
        public String commandName() {
            return command;
        }

        @Override
        public String context() {
            return context;
        }

        @Override
        public Payload payload() {
            return payload;
        }

        @Override
        public Metadata metadata() {
            return metadata;
        }
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

    private static class GrpcCommandHandler implements CommandHandler {

        private final String id;
        private final String commandSubscription;
        private final String context;
        private final Metadata metadata;

        public GrpcCommandHandler(String commandSubscription, String context, Map<String, Serializable> metadata) {
            this.context = context;
            this.id = UUID.randomUUID().toString();
            this.commandSubscription = commandSubscription;
            this.metadata = DefaultCommandRequestProcessorTest.metadata(metadata);
        }

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
            return commandSubscription;
        }

        @Override
        public String context() {
            return context;
        }

        @Override
        public Metadata metadata() {
            return metadata;
        }
    }

    private static class SimpleHandlerSelector implements HandlerSelector,
            CommandHandlerSubscribedInterceptor,
            CommandHandlerUnsubscribedInterceptor {

        private boolean subscribedInterceptorCalled;
        private boolean unsubscribedInterceptorCalled;

        public boolean subscribedInterceptorCalled() {
            return subscribedInterceptorCalled;
        }

        @Override
        public Set<CommandHandlerSubscription> select(Set<CommandHandlerSubscription> candidates, Command command) {
            return null;
        }

        @Override
        public Mono<Void> onCommandHandlerSubscribed(CommandHandler commandHandler) {
            subscribedInterceptorCalled = true;
            return Mono.empty();
        }

        @Override
        public Mono<Void> onCommandHandlerUnsubscribed(CommandHandler commandHandler) {
            unsubscribedInterceptorCalled = true;
            return Mono.empty();
        }

        public boolean unsubscribedInterceptorCalled() {
            return unsubscribedInterceptorCalled;
        }
    }
}