package io.axoniq.axonserver.eventstore.transformation.apply;

import io.axoniq.axonserver.eventstore.transformation.api.EventStoreTransformationService.Transformation;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.Transformations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DefaultTransformationApplyTask implements TransformationApplyTask {

    private static final Logger logger = LoggerFactory.getLogger(DefaultTransformationApplyTask.class);

    private final TransformationApplyExecutor applier;
    private final MarkTransformationApplied markTransformationApplied;
    private final Transformations transformations;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    public DefaultTransformationApplyTask(TransformationApplyExecutor applier,
                                          MarkTransformationApplied markTransformationApplied,
                                          Transformations transformations) {
        this.applier = applier;
        this.markTransformationApplied = markTransformationApplied;
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
                       .flatMap(transformation -> applier.apply(new ApplierTransformation(transformation))
                                                         .doOnError(t -> logger.error(
                                                                 "An error happened while applying the transformation: {}.",
                                                                 transformation.id(),
                                                                 t))
                                                         .doOnSuccess(notUsed -> logger.warn(
                                                                 "Applied transformation: {}",
                                                                 transformation.id()))
                                                         .then(markTransformationApplied.markApplied(transformation.context(),
                                                                                                     transformation.id())))
                       .doFinally(s -> scheduledExecutorService.schedule(this::apply, 10, TimeUnit.SECONDS))
                       .subscribe();
    }

    static class ApplierTransformation implements TransformationApplyExecutor.Transformation {

        private final Transformation state;

        ApplierTransformation(Transformation state) {
            this.state = state;
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
                        .orElseThrow(() -> new IllegalStateException("The last sequence of transformation is " + id()));
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
