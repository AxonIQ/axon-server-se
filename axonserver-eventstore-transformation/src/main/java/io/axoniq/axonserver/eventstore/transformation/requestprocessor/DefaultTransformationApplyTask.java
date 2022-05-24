package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import io.axoniq.axonserver.eventstore.transformation.api.EventStoreTransformationService.Transformation;
import reactor.core.publisher.Mono;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DefaultTransformationApplyTask implements TransformationApplyTask {

    private final TransformationApplyExecutor applier;
    private final Transformers transformers;
    private final Transformations transformations;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    public DefaultTransformationApplyTask(TransformationApplyExecutor applier,
                                          Transformers transformers, Transformations transformations) {
        this.applier = applier;
        this.transformers = transformers;
        this.transformations = transformations;
    }

    @Override
    public void start() {
        scheduledExecutorService.schedule(this::apply, 10, TimeUnit.SECONDS);
    }

    @Override
    public void stop() {
        scheduledExecutorService.shutdown();
    }

    private void apply() {
        transformations.applyingTransformations()
                       .flatMap(transformation -> applier.apply(new ApplierTransformation(transformation,
                                                                                          transformers)))
                       .doFinally(s -> scheduledExecutorService.schedule(this::apply, 10, TimeUnit.SECONDS))
                       .subscribe(/* TODO logger*/);
    }

    static class ApplierTransformation implements TransformationApplyExecutor.Transformation {

        private final Transformation state;
        private final Transformers transformers;

        ApplierTransformation(Transformation state, Transformers transformers) {
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
        public int version() {
            return state.version();
        }

        @Override
        public long lastSequence() {
            return state.lastSequence()
                        .get();
        }

        @Override
        public Mono<Void> markAsApplied() {
            return transformers.transformerFor(context())
                               .flatMap(transformation -> transformation.markApplied(id()));
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
