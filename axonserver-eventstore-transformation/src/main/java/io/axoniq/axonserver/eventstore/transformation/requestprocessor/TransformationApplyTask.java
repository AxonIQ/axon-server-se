package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public interface TransformationApplyTask {

    void start();

    void stop();
}

class DefaultTransformationApplyTask implements TransformationApplyTask {

    private final String context;
    private final TransformationApplier applier;
    private final ContextTransformationStore contextTransformationStore;
    private final ContextTransformer contextTransformer;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    DefaultTransformationApplyTask(String context,
                                   TransformationApplier applier,
                                   ContextTransformationStore contextTransformationStore,
                                   ContextTransformer contextTransformer) {
        this.context = context;
        this.applier = applier;
        this.contextTransformationStore = contextTransformationStore;
        this.contextTransformer = contextTransformer;
    }

    @Override
    public void start() {
        scheduledExecutorService.scheduleWithFixedDelay(this::apply, 10, 30, TimeUnit.SECONDS);
    }

    @Override
    public void stop() {

    }

    private void apply() {
        contextTransformer.restart()
                                  .subscribe();
//        contextTransformationStore.current()
//                                  .filter(transformation -> transformation.status()
//                                                                          .equals(EventStoreTransformationJpa.Status.APPLYING))
//                                  .map(transformation -> applier.apply(new ApplierTransformation(context,
//                                                                                                 transformation)))
//                .subscribe(v -> makrApplied);
    }

    class ApplierTransformation implements TransformationApplier.Transformation {

        private final TransformationState state;
        private final String context;

        ApplierTransformation(String context, TransformationState state) {
            this.context = context;
            this.state = state;
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
        public int version() {
            return state.version();
        }

        @Override
        public long lastSequence() {
            return state.lastSequence()
                        .get();
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