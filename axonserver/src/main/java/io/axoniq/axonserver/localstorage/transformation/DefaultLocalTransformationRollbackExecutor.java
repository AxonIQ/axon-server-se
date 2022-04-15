package io.axoniq.axonserver.localstorage.transformation;

import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationRollbackExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

public class DefaultLocalTransformationRollbackExecutor implements LocalTransformationRollbackExecutor {

    private static final Logger logger = LoggerFactory.getLogger(DefaultLocalTransformationRollbackExecutor.class);

    private final LocalEventStoreTransformer localEventStoreTransformer;
    private final Set<String> rollingBackTransformations = new CopyOnWriteArraySet<>();

    public DefaultLocalTransformationRollbackExecutor(LocalEventStoreTransformer localEventStoreTransformer) {
        this.localEventStoreTransformer = localEventStoreTransformer;
    }

    @Override
    public Mono<Void> rollback(Transformation transformation) {
        return Mono.fromSupplier(() -> rollingBackTransformations.add(transformation.id()))
                   .filter(inactive -> inactive)
                   .switchIfEmpty(Mono.error(new RuntimeException("The rollback operation is already in progress")))
                   .then(localEventStoreTransformer.rollback(transformation.context(), transformation.version())
                                                   .doFinally(s -> rollingBackTransformations.remove(transformation.id())))
                   .doOnSuccess(v -> logger.info("Transformation {} rolled back successfully in local store.", transformation))
                   .doOnError(t -> logger.info("Failed to rollback in the  local store the transformation {}", transformation));
    }
}
