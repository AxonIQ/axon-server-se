package io.axoniq.axonserver.eventstore.transformation.cancel;

import io.axoniq.axonserver.eventstore.transformation.api.EventStoreTransformationService;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.Transformations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DefaultTransformationCancelTask implements TransformationCancelTask {

    private static final Logger logger = LoggerFactory.getLogger(DefaultTransformationCancelTask.class);
    private final TransformationCancelExecutor cancel;
    private final MarkTransformationCancelled markTransformationCancelled;
    private final Transformations transformations;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    public DefaultTransformationCancelTask(TransformationCancelExecutor cancel,
                                           MarkTransformationCancelled markTransformationCancelled,
                                           Transformations transformations) {
        this.cancel = cancel;
        this.markTransformationCancelled = markTransformationCancelled;
        this.transformations = transformations;
    }

    @Override
    public void start() {
        scheduledExecutorService.schedule(this::cancel, 10, TimeUnit.SECONDS);
    }

    @Override
    public void stop() {
        scheduledExecutorService.shutdown();
    }

    private void cancel() {
        transformations.cancellingTransformations()
                       .doOnNext(transformation -> logger.warn("Cancelling transformation: {}", transformation.id()))
                       .flatMap(transformation -> cancel.cancel(new CancelTransformation(transformation))
                                                        .doOnError(t -> logger.error(
                                                                "An error happened while cancelling the transformation: {}.",
                                                                transformation.id(),
                                                                t))
                                                        .doOnSuccess(notUsed -> logger.warn(
                                                                "Transformation cancelled: {}",
                                                                transformation.id()))
                                                        .then(markTransformationCancelled.markCancelled(transformation.context(),
                                                                                                        transformation.id())))
                       .doFinally(s -> scheduledExecutorService.schedule(this::cancel, 10, TimeUnit.SECONDS))
                       .subscribe();
    }

    private static class CancelTransformation implements TransformationCancelExecutor.Transformation {

        private final EventStoreTransformationService.Transformation transformation;

        public CancelTransformation(EventStoreTransformationService.Transformation transformation) {
            this.transformation = transformation;
        }

        @Override
        public String id() {
            return transformation.id();
        }

        @Override
        public String context() {
            return transformation.context();
        }

        @Override
        public int version() {
            return transformation.version();
        }
    }
}
