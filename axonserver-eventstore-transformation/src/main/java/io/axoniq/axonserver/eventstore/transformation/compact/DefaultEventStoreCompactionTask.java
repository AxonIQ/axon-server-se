package io.axoniq.axonserver.eventstore.transformation.compact;

import io.axoniq.axonserver.eventstore.transformation.compact.CompactingContexts.CompactingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DefaultEventStoreCompactionTask implements EventStoreCompactionTask {

    private static final Logger logger = LoggerFactory.getLogger(DefaultEventStoreCompactionTask.class);

    private final Flux<CompactingContext> compactingContexts;
    private final EventStoreCompactionExecutor compactionExecutor;
    private final MarkEventStoreCompacted markEventStoreCompacted;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(); //TODO make it configurable


    public DefaultEventStoreCompactionTask(Flux<CompactingContext> compactingContexts,
                                           EventStoreCompactionExecutor compactionExecutor,
                                           MarkEventStoreCompacted markEventStoreCompacted) {
        this.compactingContexts = compactingContexts;
        this.compactionExecutor = compactionExecutor;
        this.markEventStoreCompacted = markEventStoreCompacted;
    }

    @Override
    public void start() {
        scheduledExecutorService.schedule(this::compact, 10, TimeUnit.SECONDS);
    }

    @Override
    public void stop() {
        scheduledExecutorService.shutdown();
    }

    private void compact() {
        compactingContexts.flatMap(compactingContext -> compactionExecutor.compact(new Compaction(compactingContext.context()))
                                                                          .then(markEventStoreCompacted.markCompacted(
                                                                                  compactingContext.compactionId(),
                                                                                  compactingContext.context())))
                          .doFinally(s -> scheduledExecutorService.schedule(this::compact,
                                                                            10,
                                                                            TimeUnit.SECONDS))
                          .subscribe(unused -> logger.info("Compacted "),
                                     t -> logger.error("Error during compaction", t));
    }

    static class Compaction implements EventStoreCompactionExecutor.Compaction {

        private final String context;

        public Compaction(String context) {
            this.context = context;
        }

        @Override
        public String context() {
            return context;
        }
    }
}
