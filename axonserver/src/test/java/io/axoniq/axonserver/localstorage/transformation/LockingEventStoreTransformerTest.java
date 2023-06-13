package io.axoniq.axonserver.localstorage.transformation;

import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.localstorage.EventStoreLockProvider;
import io.axoniq.axonserver.util.IdLock;
import org.junit.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class LockingEventStoreTransformerTest {

    private EventStoreLockProvider eventStoreLockProvider;
    private EventStoreTransformer eventStoreTransformer;

    @Before
    public void setUp() {
        eventStoreLockProvider = new EventStoreLockProvider();
        EventStoreTransformer delegate = new EventStoreTransformer() {
            @Override
            public Flux<Long> transformEvents(String context, int version, Flux<EventWithToken> transformedEvents) {
                return Flux.empty();
            }

            @Override
            public Mono<Void> compact(String context) {
                return Mono.empty();
            }
        };
        eventStoreTransformer = new LockingEventStoreTransformer(eventStoreLockProvider, delegate);
    }

    @Test
    public void testSingleResourceAccessToSameContext() {
        IdLock lock = eventStoreLockProvider.apply("MyContext");
        IdLock.Ticket ticket1 = lock.request("Multi-Tier");
        IdLock.Ticket ticket2 = lock.request("Multi-Tier");

        StepVerifier.create(eventStoreTransformer.compact("MyContext"))
                    .expectError()
                    .verify();

        ticket2.release();

        StepVerifier.create(eventStoreTransformer.compact("MyContext"))
                    .expectError()
                    .verify();

        ticket1.release();

        StepVerifier.create(eventStoreTransformer.compact("MyContext"))
                    .verifyComplete();
    }

    @Test
    public void testMultipleResourcesAccessToDifferentContexts() {
        IdLock lock = eventStoreLockProvider.apply("MyContext");
        IdLock.Ticket ticket = lock.request("Multi-Tier");

        StepVerifier.create(eventStoreTransformer.compact("AnotherContext"))
                    .verifyComplete();

        ticket.release();

        StepVerifier.create(eventStoreTransformer.compact("AnotherContext"))
                    .verifyComplete();

        StepVerifier.create(eventStoreTransformer.compact("MyContext"))
                    .verifyComplete();
    }
}