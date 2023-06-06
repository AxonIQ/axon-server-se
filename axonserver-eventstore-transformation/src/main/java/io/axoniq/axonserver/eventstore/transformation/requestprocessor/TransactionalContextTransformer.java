package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import io.axoniq.axonserver.eventstore.transformation.requestprocessor.EventStoreStateStore.EventStoreState;
import io.axoniq.axonserver.grpc.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import reactor.core.publisher.Mono;

import java.util.function.Function;

import static java.lang.String.format;

/**
 * Implementation of {@link ContextTransformer} that perform the actions in a transactional way.
 *
 * @author Milan Savic
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public class TransactionalContextTransformer implements ContextTransformer {

    private static final Logger logger = LoggerFactory.getLogger(TransactionalContextTransformer.class);
    private final String context;
    private final ContextTransformationStore transformationStore;
    private final PlatformTransactionManager platformTransactionManager;
    private final EventStoreStateStore contextStore;
    private final TransformationStateConverter converter;

    /**
     * Construct an instance based on the specified parameters.
     *
     * @param context                    the context of the event transformations
     * @param store                      the store for the event transformation
     * @param platformTransactionManager the transaction manager
     * @param eventStoreStateStore       the store for the state of the event store
     * @param converter                  the transformation converter
     */
    TransactionalContextTransformer(String context, ContextTransformationStore store,
                                    PlatformTransactionManager platformTransactionManager,
                                    EventStoreStateStore eventStoreStateStore,
                                    TransformationStateConverter converter) {
        this.context = context;
        this.transformationStore = store;
        this.platformTransactionManager = platformTransactionManager;
        this.contextStore = eventStoreStateStore;
        this.converter = converter;
    }

    private Mono<Void> executeInTransaction(Runnable runnable) {
        return Mono.create(sink -> {
            TransactionStatus transactionStatus = platformTransactionManager.getTransaction(TransactionDefinition.withDefaults());
            try {
                runnable.run();
                platformTransactionManager.commit(transactionStatus);
                sink.success();
            } catch (Exception e) {
                logger.warn("Rolling back the event transformation transaction for context {}. Reason: ", context, e);
                platformTransactionManager.rollback(transactionStatus);
                sink.error(e);
            }
        });
    }

    @Override
    public Mono<Void> start(String id, String description) {
        return executeInTransaction(() -> {
            EventStoreState state = contextStore.state(context);
            contextStore.save(state.transform(id));
            transformationStore.create(id, description);
        });
    }

    @Override
    public Mono<Void> deleteEvent(String transformationId, long tokenToDelete, long sequence) {
        return perform(transformationId,
                       "DELETE_EVENT",
                       transformation -> transformation.deleteEvent(tokenToDelete, sequence))
                .doFirst(() -> logger.info("Performing DELETE EVENT action."));
    }

    @Override
    public Mono<Void> replaceEvent(String transformationId, long token, Event event, long sequence) {
        return perform(transformationId,
                       "REPLACE_EVENT",
                       transformation -> transformation.replaceEvent(token, event, sequence));
    }

    @Override
    public Mono<Void> cancel(String transformationId) {
        return transformationStore.transformation(transformationId) // state interface (implemented by jpa) #with(...)
                                  .flatMap(converter::from)
                                  .checkpoint("Starting CANCEL action")
                                  .flatMap(Transformation::cancel)
                                  .checkpoint("Action CANCEL completed")
                                  .flatMap(transformationState -> executeInTransaction(
                                          () -> {
                                              transformationStore.save(transformationState);
                                              EventStoreState storeState = contextStore.state(context);
                                              contextStore.save(storeState.cancelled());
                                          }))
                                  .checkpoint("Transformation updated after CANCEL")
                                  .then();
    }

    @Override
    public Mono<Void> startApplying(String transformationId, long sequence, String applier) {
        return perform(transformationId,
                       "START_APPLYING_TRANSFORMATION",
                       transformation -> transformation.startApplying(sequence, applier));
    }

    @Override
    public Mono<Void> markApplied(String transformationId) {
        return transformationStore.transformation(transformationId) // state interface (implemented by jpa) #with(...)
                                  .flatMap(converter::from)
                                  .checkpoint("Starting MARK_AS_APPLIED action")
                                  .flatMap(Transformation::markApplied)
                                  .checkpoint("Action MARK_AS_APPLIED completed")
                                  .flatMap(transformationState -> executeInTransaction(
                                          () -> {
                                              transformationStore.save(transformationState);
                                              EventStoreState storeState = contextStore.state(context);
                                              contextStore.save(storeState.transformed());
                                          }))
                                  .checkpoint("Transformation updated after MARK_AS_APPLIED")
                                  .then();
    }

    @Override
    public Mono<Void> startCompacting(String compactionId) {
        return executeInTransaction(() -> {
            EventStoreState state = contextStore.state(context);
            contextStore.save(state.compact(compactionId));
        });
    }

    @Override
    public Mono<Void> markCompacted(String compactionId) {
        return executeInTransaction(() -> {
            EventStoreState state = contextStore.state(context);
            contextStore.save(state.compacted());
        });
    }

    @Override
    public Mono<Void> clean() {
        return executeInTransaction(() -> {
            contextStore.clean(context);
            transformationStore.clean(context);
        });
    }


    private Mono<Void> perform(String transformationId,
                               String actionName,
                               Function<Transformation, Mono<TransformationState>> action) {
        return transformationStore.transformation(transformationId) // state interface (implemented by jpa) #with(...)
                                  .flatMap(converter::from)
                                  .checkpoint(format("Starting %s action", actionName))
                                  .flatMap(action)
                                  .checkpoint(format("Action %s completed", actionName))
                                  .flatMap(transformationState -> executeInTransaction(
                                          () -> transformationStore.save(transformationState)))
                                  .checkpoint(format("Transformation updated after %s", actionName))
                                  .then();
    }
}
