package io.axoniq.axonserver.eventstore.transformation.apply;

import io.axoniq.axonserver.eventstore.transformation.api.EventStoreTransformationService.Transformation;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.Transformations;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.Transformers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import sun.plugin.dom.exception.InvalidStateException;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DefaultTransformationApplyTask implements TransformationApplyTask {

    private static final Logger logger = LoggerFactory.getLogger(DefaultTransformationApplyTask.class);

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
                       .doOnNext(transformation -> logger.warn("Applying transformation: {}", transformation.id()))
                       .flatMap(transformation -> applier.apply(new ApplierTransformation(transformation,
                                                                                          transformers))
                                                         .doOnError(t -> logger.error(
                                                                 "An error happened while applying the transformation: {}.",
                                                                 transformation.id(),
                                                                 t))
                                                         .doOnSuccess(notUsed -> logger.warn(
                                                                 "Applied transformation: {}",
                                                                 transformation.id())))
                       .doFinally(s -> scheduledExecutorService.schedule(this::apply, 10, TimeUnit.SECONDS))
                       .subscribe();
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
                        .orElseThrow(() -> new InvalidStateException("The last sequence of transformation is " + id()));
        }

        @Override
        public Mono<Void> markAsApplied() {
            return transformers.transformerFor(context())
                               .doOnNext(unused -> logger.warn("Marking as applied transformation {}", id()))
                               .flatMap(transformation -> transformation.markApplied(id()))
                               .doOnSuccess(unused -> logger.warn("Transformation {} marked applied.", id()));
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
