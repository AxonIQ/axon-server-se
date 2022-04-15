package io.axoniq.axonserver.localstorage;

import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.eventstore.transformation.ReplaceEvent;
import io.axoniq.axonserver.eventstore.transformation.TransformationAction;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationApplier;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationEntry;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationEntryStore;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.localstorage.file.TransformationProgress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

public class LocalTransformationApplier implements TransformationApplier {

    private static final Logger logger = LoggerFactory.getLogger(LocalTransformationApplier.class);

    private final TransformationEntryStore transformationEntryStore;
    private final LocalTransformationProgressStore stateRepo;
    private final LocalEventStoreTransformer transformer;
    private final Set<String> applyingTransformations = new CopyOnWriteArraySet<>();

    public LocalTransformationApplier(TransformationEntryStore transformationEntryStore,
                                      LocalTransformationProgressStore stateRepo,
                                      LocalEventStoreTransformer transformer) {
        this.transformationEntryStore = transformationEntryStore;
        this.stateRepo = stateRepo;
        this.transformer = transformer;
    }

    @Override
    public Mono<Void> apply(Transformation transformation) {
        Flux<EventWithToken> transformedEvents =
                stateRepo.stateFor(transformation.id())
                         .map(state -> state.lastAppliedSequence() + 1)
                         .flatMapMany(firstSequence -> transformationEntryStore.read(firstSequence,
                                                                                     transformation.lastSequence()))
                         .map(TransformationEntry::payload)
                         .flatMapSequential(this::parseFrom)
                         .map(this::eventWithToken);
        return Mono.fromSupplier(() -> applyingTransformations.add(transformation.id()))
                   .filter(inactive -> inactive)
                   .switchIfEmpty(Mono.error(new RuntimeException("applying already in progress")))
                   .then(transformer.transformEvents(transformation.context(),
                                                     transformation.version(),
                                                     transformedEvents)
                                    .flatMapSequential(progress -> sequence(transformation.id(), progress))
                                    .flatMapSequential(lastProcessedSequence -> stateRepo.updateLastSequence(
                                            transformation.id(),
                                            lastProcessedSequence))
                                    .then(stateRepo.markAsApplied(transformation.id()))
                                    .doFinally(onFinally -> applyingTransformations.remove(transformation.id())))
                   .doOnSuccess(v -> logger.info("Transformation {} applied successfully to local store.", transformation))
                   .doOnError(t -> logger.info("Failed to apply to local store the transformation {}", transformation));
    }

    @Override
    public Mono<Long> lastAppliedSequence(String transformationId) {
        return stateRepo.stateFor(transformationId)
                        .map(TransformationApplyingState::lastAppliedSequence);
    }

    private Mono<Long> sequence(String transformationId, TransformationProgress progress) {
        return stateRepo.stateFor(transformationId)
                        .map(state -> state.lastAppliedSequence() + progress.eventsTransformed());
    }

    private EventWithToken eventWithToken(TransformationAction transformationAction) {
        if (transformationAction.hasDeleteEvent()) {
            long token = transformationAction.getDeleteEvent()
                                             .getToken();
            return eventWithToken(Event.getDefaultInstance(), token);
        } else {
            ReplaceEvent replaceEvent = transformationAction.getReplaceEvent();
            Event event = replaceEvent.getEvent();
            long token = replaceEvent.getToken();
            return eventWithToken(event, token);
        }
    }

    private EventWithToken eventWithToken(Event event, long token) {
        return EventWithToken.newBuilder()
                             .setEvent(event)
                             .setToken(token)
                             .build();
    }

    private Mono<TransformationAction> parseFrom(byte[] data) {
        return Mono.create(sink -> {
            try {
                sink.success(TransformationAction.parseFrom(data));
            } catch (InvalidProtocolBufferException e) {
                sink.error(e);
            }
        });
    }
}

interface LocalTransformationProgressStore {

    /**
     * It will never return an empty Mono, because if this is empty the default initial state is returned.
     *
     * @param transformationId the identifier of the transformation
     * @return
     */
    Mono<TransformationApplyingState> stateFor(String transformationId);

    Mono<Void> updateLastSequence(String transformationId, long lastProcessedSequence);

    Mono<Void> markAsApplied(String transformationId);
}

interface TransformationApplyingState {

    long lastAppliedSequence();

    boolean applied();
}
