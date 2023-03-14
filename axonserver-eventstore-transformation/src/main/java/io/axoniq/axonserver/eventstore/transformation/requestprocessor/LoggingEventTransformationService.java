package io.axoniq.axonserver.eventstore.transformation.requestprocessor;

import io.axoniq.axonserver.api.Authentication;
import io.axoniq.axonserver.eventstore.transformation.api.EventStoreTransformationService;
import io.axoniq.axonserver.grpc.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public class LoggingEventTransformationService implements EventStoreTransformationService {

    private static final Logger logger = LoggerFactory.getLogger(LoggingEventTransformationService.class);
    private final EventStoreTransformationService delegate;

    public LoggingEventTransformationService(EventStoreTransformationService delegate) {
        this.delegate = delegate;
    }


    @Override
    public Mono<Void> start(String id, String context, String description, @Nonnull Authentication authentication) {
        return delegate.start(id, context, description, authentication)
                       .doFirst(() -> logger.info("Starting transformation {} with description {} for context {}.",
                                                  id, description, context))
                       .doOnSuccess(unused -> logger.info("Transformation {} for context {} started.",
                                                          id, context))
                       .doOnError(error -> logger.warn("Error starting transformation {} for context {}",
                                                       id, context, error));
    }

    @Override
    public Flux<Transformation> transformations(String context, @Nonnull Authentication authentication) {
        return delegate.transformations(context, authentication)
                       .doFirst(() -> logger.info("Loading transformation for context {}.", context))
                       .doOnComplete(() -> logger.info("Provided transformations for context {}.", context))
                       .doOnError(error -> logger.warn("Error providing transformations for context {}",
                                                       context,
                                                       error));
    }

    @Override
    public Mono<Void> replaceEvent(String context, String transformationId, long token, Event event, long sequence,
                                   @Nonnull Authentication authentication) {
        return delegate.replaceEvent(context, transformationId, token, event, sequence, authentication)
                       .doFirst(() -> logger.trace(
                               "Replacing event with event token {} for transformation {} and context {}. The received sequence of the transformation action: {}.",
                               token,
                               transformationId,
                               context,
                               sequence))
                       .doOnSuccess(unused -> logger.trace(
                               "Replace action with event token {} for transformation {} and context {} accepted. The received sequence of the transformation action: {}.",
                               token,
                               transformationId,
                               context,
                               sequence))
                       .doOnError(error -> logger.warn(
                               "There was an error while trying to accept replace action with event token {} for transformation {} and context {}. The received sequence of the transformation action: {}.",
                               token,
                               transformationId,
                               context,
                               sequence,
                               error));
    }

    @Override
    public Mono<Void> deleteEvent(String context, String transformationId, long token, long sequence,
                                  @Nonnull Authentication authentication) {
        return delegate.deleteEvent(context, transformationId, token, sequence, authentication)
                       .doFirst(() -> logger.trace(
                               "Saving delete event operation for token {} in transformation {} for context {}. The received sequence of the transformation action is: {}.",
                               token,
                               transformationId,
                               context,
                               sequence))
                       .doOnSuccess(unused -> logger.trace(
                               "Saved delete event operation for token {} in transformation {} for context {}. The received sequence of the transformation action is: {}.",
                               token,
                               transformationId,
                               context,
                               sequence))
                       .doOnError(error -> logger.warn(
                               "Error Saving delete event operation for token {} in transformation {} for context {}. The received sequence of the transformation action is: {}.",
                               token,
                               transformationId,
                               context,
                               sequence,
                               error));
    }

    @Override
    public Mono<Void> startCompacting(String compactionId, String context, @Nonnull Authentication authentication) {
        return delegate.startCompacting(compactionId, context, authentication)
                       .doFirst(() -> logger.info(
                               "Marking the event store for context {} to be compacted. Operation id: {}.",
                               context,
                               compactionId))
                       .doOnSuccess(unused -> logger.info(
                               "Event store for context {} marked to be compacted. Operation id: {}",
                               context,
                               compactionId))
                       .doOnError(error -> logger.warn(
                               "Error marking the event store for context {} to be compacted. Operation id: {}",
                               context,
                               compactionId,
                               error));
    }

    @Override
    public Mono<Void> cancel(String context, String transformationId, @Nonnull Authentication authentication) {
        return delegate.cancel(context, transformationId, authentication)
                       .doFirst(() -> logger.info("Cancelling transformation {} for context {}.",
                                                  transformationId, context))
                       .doOnSuccess(unused -> logger.info("Transformation {} for context {} has been cancelled.",
                                                          transformationId, context))
                       .doOnError(error -> logger.warn("Error cancelling transformation {} for context {}.",
                                                       transformationId, context, error));
    }

    @Override
    public Mono<Void> startApplying(String context, String transformationId, long sequence,
                                    @Nonnull Authentication authentication) {
        return delegate.startApplying(context, transformationId, sequence, authentication)
                       .doFirst(() -> logger.info(
                               "Starting apply process for the transformation {} and context {}. The received sequence of the transformation is: {}.",
                               transformationId,
                               context,
                               sequence))
                       .doOnSuccess(unused -> logger.info(
                               "Apply process for the transformation {} and context {} started. The received sequence of the transformation is: {}.",
                               transformationId,
                               context,
                               sequence))
                       .doOnError(error -> logger.warn(
                               "There was an error starting the apply process for the transformation {} and context {}. The received sequence of the transformation is: {}",
                               transformationId,
                               context,
                               sequence,
                               error));
    }
}
