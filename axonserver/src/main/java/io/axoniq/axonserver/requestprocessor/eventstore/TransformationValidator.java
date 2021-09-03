package io.axoniq.axonserver.requestprocessor.eventstore;

import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.localstorage.SerializedEventWithToken;
import org.springframework.data.util.CloseableIterator;
import org.springframework.stereotype.Controller;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Validates event store transformation requests.
 * There can only be one transformation per context.
 * Each entry should provide a valid previous token.
 * When an event is replaced, the aggregate id and sequence number must remain the same.
 * @author Marc Gathier
 * @since 4.6.0
 */
@Controller
public class TransformationValidator {

    private final Map<String, EventStoreTransformation> activeTransformations = new ConcurrentHashMap<>();
    private final LocalEventStore localEventStore;

    public TransformationValidator(LocalEventStore localEventStore) {
        this.localEventStore = localEventStore;
    }

    public String register(String context) {
        return activeTransformations.compute(context, (c, current) -> {
            if (current != null) {
                throw new RuntimeException("Transformation in progress");
            }
            EventStoreTransformation eventStoreTransformation = new EventStoreTransformation();
            eventStoreTransformation.setId(UUID.randomUUID().toString());
            eventStoreTransformation.setName(context);
            return eventStoreTransformation;
        }).getId();
    }


    public void validateDeleteEvent(String context, String transformationId, long token, long previousToken) {
        EventStoreTransformation transformation = activeTransformations.get(context);
        validateTransformationId(transformationId, transformation);
        validatePreviousToken(previousToken, transformation);

        transformation.setPreviousToken(token);
    }

    public void validateReplaceEvent(String context, String transformationId, long token, long previousToken,
                                     Event event) {
        EventStoreTransformation transformation = activeTransformations.get(context);
        validateTransformationId(transformationId, transformation);
        validatePreviousToken(previousToken, transformation);
        validateEventToReplace(token, event, context, transformation);
        transformation.setPreviousToken(token);
    }

    public void apply(String context, String transformationId, long lastEventToken) {
        activeTransformations.compute(context, (c, old) -> {
            validateTransformationId(transformationId, old);
            validatePreviousToken(lastEventToken, old);

            if (old.isApplying()) {
                throw new RuntimeException("Transformation in progress");
            }

            old.setApplying(true);
            old.closeIterator();
            return old;
        });
    }

    public void cancel(String context, String transformationId) {
        activeTransformations.compute(context, (c, old) -> {
            if( old == null) {
                return null;
            }
            validateTransformationId(transformationId, old);

            if (old.isApplying()) {
                throw new RuntimeException("Transformation in progress");
            }
            old.closeIterator();
            return null;
        });
    }

    private void validatePreviousToken(long previousToken, EventStoreTransformation transformation) {
        if (previousToken != transformation.getPreviousToken()) {
            throw new RuntimeException("Invalid previous token");
        }
    }

    private void validateTransformationId(String transformationId, EventStoreTransformation transformation) {
        if (transformation == null) {
            throw new RuntimeException("Transformation not found");
        }

        if (!transformation.getId().equals(transformationId)) {
            throw new RuntimeException("Transformation id not valid");
        }
    }

    private void validateEventToReplace(long token, Event event, String context,
                                        EventStoreTransformation transformation) {
        if (event.getAggregateType().isEmpty()) {
            return;
        }

        CloseableIterator<SerializedEventWithToken> iterator = transformation.getIterator();
        if (iterator == null) {
            iterator = localEventStore.eventIterator(context, token);
            transformation.setIterator(iterator);
        }

        if (!iterator.hasNext()) {
            throw new RuntimeException("Event for token not found: " + token);
        }
        SerializedEventWithToken stored = iterator.next();
        while (stored.getToken() < token && iterator.hasNext()) {
            stored = iterator.next();
        }

        if (stored.getToken() != token) {
            throw new RuntimeException("Event for token not found: " + token);
        }

        if (!event.getAggregateIdentifier().equals(stored.getSerializedEvent().getAggregateIdentifier())) {
            throw new RuntimeException("Invalid aggregate identifier for: " + token);
        }
        if (event.getAggregateSequenceNumber() != stored.getSerializedEvent()
                                                        .getAggregateSequenceNumber()) {
            throw new RuntimeException("Invalid aggregate sequence number for: " + token);
        }
    }
}
