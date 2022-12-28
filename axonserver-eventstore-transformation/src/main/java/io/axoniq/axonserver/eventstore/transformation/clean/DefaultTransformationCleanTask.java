package io.axoniq.axonserver.eventstore.transformation.clean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DefaultTransformationCleanTask implements TransformationCleanTask {

    private static final Logger logger = LoggerFactory.getLogger(DefaultTransformationCleanTask.class);
    private final TransformationCleanExecutor cleanExecutor;
    private final TransformationsToBeCleaned transformationsToBeCleaned;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    public DefaultTransformationCleanTask(TransformationCleanExecutor cleanExecutor,
                                          TransformationsToBeCleaned transformationsToBeCleaned) {
        this.cleanExecutor = cleanExecutor;
        this.transformationsToBeCleaned = transformationsToBeCleaned;
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
        transformationsToBeCleaned
                .toBeCleaned()
                .doOnNext(transformation -> logger.info("Cleaning transformation: {}", transformation.id()))
                .flatMap(transformation -> cleanExecutor.clean(transformation.context(), transformation.id())
                                                        .then(transformation.markAsCleaned())
                                                        .doOnError(t -> logger.error(
                                                                "An error happened while cleaning the transformation: {}.",
                                                                transformation.id(),
                                                                t))
                                                        .doOnSuccess(notUsed -> logger.info(
                                                                "Transformation cleaned: {}",
                                                                transformation.id())))
                .doFinally(s -> scheduledExecutorService.schedule(this::cancel, 10, TimeUnit.SECONDS))
                .subscribe();
    }
}
