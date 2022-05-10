package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import io.axoniq.axonserver.eventstore.transformation.api.EventStoreTransformationService;
import reactor.core.publisher.Mono;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DefaultTransformationRollBackTask implements TransformationRollBackTask {

    private final TransformationRollbackExecutor rollbackExecutor;
    private final Transformations transformations;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    private final Transformers transformers;

    public DefaultTransformationRollBackTask(TransformationRollbackExecutor rollbackExecutor,
                                             Transformations transformations,
                                             Transformers transformers) {
        this.rollbackExecutor = rollbackExecutor;
        this.transformations = transformations;
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
        transformations.rollingBackTransformations()
                       .map(transformation -> rollbackExecutor.rollback(new Transformation(transformation,
                                                                                           transformers)))
                       .doFinally(s -> scheduledExecutorService.schedule(this::rollback,
                                                                         10,
                                                                         TimeUnit.SECONDS))
                       .subscribe(/*TODO logger*/);
    }

    static class Transformation implements TransformationRollbackExecutor.Transformation {

        private final EventStoreTransformationService.Transformation state;
        private final Transformers transformers;

        Transformation(EventStoreTransformationService.Transformation state, Transformers transformers) {
            this.state = state;
            this.transformers = transformers;
        }

        @Override
        public String id() {
            return state.id();
        }

        @Override
        public String context() {
            return state.context();
        }

        @Override
        public Mono<Void> markRolledBack() {
            return transformers.transformerFor(context())
                               .flatMap(transformer -> transformer.markRolledBack(id()));
        }

        @Override
        public int version() {
            return state.version();
        }

        @Override
        public String toString() {
            return "ApplierTransformation{" +
                    "state=" + state +
                    ", context='" + context() + '\'' +
                    '}';
        }
    }
}
