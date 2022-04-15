package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.axoniq.axonserver.eventstore.transformation.requestprocessor.EventStoreTransformationJpa.Status.ROLLING_BACK;

public class DefaultTransformationRollBackTask implements TransformationRollBackTask {

    private final String context;
    private final TransformationRollbackExecutor rollbackExecutor;
    private final ContextTransformationStore contextTransformationStore;
    private final Flux<TransformationState> currentTransformations;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    private final Transformers transformers;

    public DefaultTransformationRollBackTask(String context,
                                             TransformationRollbackExecutor rollbackExecutor,
                                             ContextTransformationStore contextTransformationStore,
                                             Transformers transformers) {
        this.context = context;
        this.rollbackExecutor = rollbackExecutor;
        this.contextTransformationStore = contextTransformationStore;
        this.transformers = transformers;
    }

    @Override
    public void start() {
        scheduledExecutorService.schedule(this::rollback, 10, TimeUnit.SECONDS);
    }

    @Override
    public void stop() {
        scheduledExecutorService.shutdown();
    }

    private void rollback() {
        currentTransformations
                                  .filter(transformation -> transformation.status().equals(ROLLING_BACK))
                                  .map(transformation -> rollbackExecutor.rollback(new Transformation(context,
                                                                                                      transformation,
                                                                                                      transformers)))
                                  .doFinally(s -> scheduledExecutorService.schedule(this::rollback,
                                                                                    10,
                                                                                    TimeUnit.SECONDS))
                                  .subscribe(/*TODO logger*/);
    }

    static class Transformation implements TransformationRollbackExecutor.Transformation {

        private final TransformationState state;
        private final String context;
        private final Transformers transformers;

        Transformation(String context, TransformationState state, Transformers transformers) {
            this.context = context;
            this.state = state;
            this.transformers = transformers;
        }

        @Override
        public String id() {
            return state.id();
        }

        @Override
        public String context() {
            return context;
        }

        @Override
        public Mono<Void> markRolledback() {
            return transformers.transformerFor(context())
                               .markRolledBack(id());
        }

        @Override
        public int version() {
            return state.version();
        }

        @Override
        public String toString() {
            return "ApplierTransformation{" +
                    "state=" + state +
                    ", context='" + context + '\'' +
                    '}';
        }
    }
}
