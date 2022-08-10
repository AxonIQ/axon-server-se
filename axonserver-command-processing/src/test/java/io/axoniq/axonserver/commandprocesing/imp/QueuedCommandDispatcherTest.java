package io.axoniq.axonserver.commandprocesing.imp;

import io.axoniq.axonserver.commandprocessing.spi.Command;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandler;
import io.axoniq.axonserver.commandprocessing.spi.CommandHandlerSubscription;
import io.axoniq.axonserver.commandprocessing.spi.CommandResult;
import io.axoniq.axonserver.commandprocessing.spi.Metadata;
import io.axoniq.axonserver.commandprocessing.spi.Payload;
import io.axoniq.axonserver.commandprocessing.spi.ResultPayload;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.io.Serializable;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class QueuedCommandDispatcherTest {

    private final QueuedCommandDispatcher testSubject = new QueuedCommandDispatcher(Schedulers.boundedElastic(),
                                                                                    h -> Optional.of("clientId"),
                                                                                    100,
                                                                                    5000,
                                                                                    new SimpleMeterRegistry());

    @Test
    public void dispatch() throws InterruptedException {
        CommandHandlerSubscription handler = commandHandlerSubscription();
        Command request = request("request1");

        StepVerifier.create(testSubject.dispatch(handler, request))
                    .expectSubscription()
                    .expectNoEvent(Duration.ofMillis(100))
                    .then(() -> testSubject.request("clientId", 10))
                    .expectNextMatches(response -> response.commandId().equals(request.id()))
                    .verifyComplete();

        Command request2 = request("request2");
        StepVerifier.create(testSubject.dispatch(handler, request2))
                    .expectSubscription()
                    .expectNextMatches(response -> response.commandId().equals(request2.id()))
                    .verifyComplete();
    }

    @Test
    public void dispatchTimeout() throws InterruptedException {
        CommandHandlerSubscription handler = commandHandlerSubscription();
        Command request = request("request1");
        testSubject.dispatch(handler, request)
                   .subscribe(r -> {
                   }, Throwable::printStackTrace);
        Thread.sleep(100);
        testSubject.timeout();
        testSubject.request("clientId", 10);
        Thread.sleep(100);
    }

    @Test
    public void unsubscribe() throws InterruptedException {
        CommandHandlerSubscription handler = commandHandlerSubscription();
        Command request = request("request1");
        CompletableFuture<CommandResult> futureResult = testSubject.dispatch(handler, request)
                                                                   .toFuture();
        testSubject.onCommandHandlerUnsubscribed(handler.commandHandler()).block();
        assertTrue(futureResult.isCompletedExceptionally());
        try {
            futureResult.get();
            fail("Unexpected result");
        } catch (ExecutionException ex) {
            assertTrue(ex.getCause() instanceof RequestDequeuedException);
        }
    }

    AtomicBoolean dispatched = new AtomicBoolean();
    private CommandHandlerSubscription commandHandlerSubscription() {
        return new CommandHandlerSubscription() {
            @Override
            public CommandHandler commandHandler() {
                return new CommandHandler() {
                    @Override
                    public String id() {
                        return "id";
                    }

                    @Override
                    public String description() {
                        return null;
                    }

                    @Override
                    public String commandName() {
                        return "commandName";
                    }

                    @Override
                    public String context() {
                        return "context";
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

            @Override
            public Mono<CommandResult> dispatch(Command command) {
                System.out.println("Dispatch: " + command.id());
                return Mono.just(new CommandResult() {
                    @Override
                    public String id() {
                        return "resultId";
                    }

                    @Override
                    public String commandId() {
                        return command.id();
                    }

                    @Override
                    public ResultPayload payload() {
                        return null;
                    }

                    @Override
                    public Metadata metadata() {
                        return new Metadata() {
                            @Override
                            public Iterable<String> metadataKeys() {
                                return Collections.emptySet();
                            }

                            @Override
                            public <R extends Serializable> Optional<R> metadataValue(String metadataKey) {
                                return Optional.empty();
                            }
                        };
                    }
                });
            }
        };
    }

    private Command request(String id) {
        return new Command() {
            @Override
            public String id() {
                return id;
            }

            @Override
            public String commandName() {
                return "commandName";
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
                return new Metadata() {
                    @Override
                    public Iterable<String> metadataKeys() {
                        return Collections.singleton(Command.TIMEOUT);
                    }

                    @Override
                    public <R extends Serializable> Optional<R> metadataValue(String metadataKey) {
                        return Command.TIMEOUT.equals(metadataKey) ? (Optional<R>) Optional.of(10L) : Optional.empty();
                    }
                };
            }
        };
    }
}