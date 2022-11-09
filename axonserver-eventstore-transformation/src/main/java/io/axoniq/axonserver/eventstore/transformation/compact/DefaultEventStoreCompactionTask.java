package io.axoniq.axonserver.eventstore.transformation.compact;

import io.axoniq.axonserver.eventstore.transformation.requestprocessor.ContextTransformer;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.Transformers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DefaultEventStoreCompactionTask implements EventStoreCompactionTask {

    private static final Logger logger = LoggerFactory.getLogger(DefaultEventStoreCompactionTask.class);

    private final Flux<String> compactingContexts;
    private final EventStoreCompactionExecutor compactionExecutor;
    private final Transformers transformers;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(); //TODO make it configurable


    public DefaultEventStoreCompactionTask(Flux<String> compactingContexts,
                                           EventStoreCompactionExecutor compactionExecutor,
                                           Transformers transformers) {
        this.compactingContexts = compactingContexts;
        this.compactionExecutor = compactionExecutor;
        this.transformers = transformers;
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
        compactingContexts.flatMap(context -> compactionExecutor.compact(new Compaction(context, transformers)))
                          .doFinally(s -> scheduledExecutorService.schedule(this::compact,
                                                                            10,
                                                                            TimeUnit.SECONDS))
                          .subscribe(unused -> logger.info("Compacted "),
                                     t -> logger.error("Error during compaction", t));
    }

    static class Compaction implements EventStoreCompactionExecutor.Compaction {

        private final String context;
        private final Transformers transformers;

        public Compaction(String context, Transformers transformers) {
            this.context = context;
            this.transformers = transformers;
        }

        @Override
        public String context() {
            return context;
        }

        @Override
        public Mono<Void> markCompacted() {
            return transformers.transformerFor(context())
                               .flatMap(ContextTransformer::markCompacted);
        }
    }
}
