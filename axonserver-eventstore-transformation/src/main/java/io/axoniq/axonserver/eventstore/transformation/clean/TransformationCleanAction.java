package io.axoniq.axonserver.eventstore.transformation.clean;

import io.axoniq.axonserver.eventstore.transformation.ActionSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class TransformationCleanAction implements ActionSupplier {

    private static final Logger logger = LoggerFactory.getLogger(TransformationCleanAction.class);
    private final TransformationCleanExecutor cleanExecutor;
    private final TransformationsToBeCleaned transformationsToBeCleaned;

    public TransformationCleanAction(TransformationCleanExecutor cleanExecutor,
                                     TransformationsToBeCleaned transformationsToBeCleaned) {
        this.cleanExecutor = cleanExecutor;
        this.transformationsToBeCleaned = transformationsToBeCleaned;
    }

    @Override
    public Mono<Void> get() {
        return transformationsToBeCleaned
                .get()
                .doOnNext(transformation -> logger.info("Cleaning transformation: {}", transformation.id()))
                .flatMap(transformation -> cleanExecutor.clean(transformation.context(), transformation.id())
                                                        .doOnError(t -> logger.error(
                                                                "An error happened while cleaning the transformation: {}.",
                                                                transformation.id(),
                                                                t))
                                                        .doOnSuccess(notUsed -> logger.info(
                                                                "Transformation cleaned: {}",
                                                                transformation.id())))
                .then();
    }
}
