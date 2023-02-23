package io.axoniq.axonserver.eventstore.transformation.compact;

import io.axoniq.axonserver.eventstore.transformation.ActionSupplier;
import io.axoniq.axonserver.eventstore.transformation.compact.CompactingContexts.CompactingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class EventStoreCompactAction implements ActionSupplier {

    private static final Logger logger = LoggerFactory.getLogger(EventStoreCompactAction.class);

    private final Flux<CompactingContext> compactingContexts;
    private final EventStoreCompactionExecutor compactionExecutor;
    private final MarkEventStoreCompacted markEventStoreCompacted;

    public EventStoreCompactAction(Flux<CompactingContext> compactingContexts,
                                   EventStoreCompactionExecutor compactionExecutor,
                                   MarkEventStoreCompacted markEventStoreCompacted) {
        this.compactingContexts = compactingContexts;
        this.compactionExecutor = compactionExecutor;
        this.markEventStoreCompacted = markEventStoreCompacted;
    }

    @Override
    public Mono<Void> get() {
        return compactingContexts.flatMap(compactingContext -> compactionExecutor.compact(new Compaction(
                                                                                         compactingContext.context()))
                                                                                 .then(markEventStoreCompacted.markCompacted(
                                                                                         compactingContext.compactionId(),
                                                                                         compactingContext.context()))
                                                                                 .doOnSuccess(blah -> logger.info(
                                                                                         "'{}' context compacted.",
                                                                                         compactingContext.context()))
                                 )
                                 .doOnError(throwable -> logger.error("Error during compaction", throwable))
                                 .then();
    }

    static class Compaction implements EventStoreCompactionExecutor.Compaction {

        private final String context;

        Compaction(String context) {
            this.context = context;
        }

        @Override
        public String context() {
            return context;
        }
    }
}
