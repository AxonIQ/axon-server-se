package io.axoniq.axonserver.message.event;

import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Stream observer wrapper that checks validity of aggregate sequence numbers.
 *
 * @author Sara Pellegrini
 * @since 4.4.11
 */
public class SequenceValidationStreamObserver implements StreamObserver<SerializedEvent> {

    private final StreamObserver<SerializedEvent> delegate;
    private final AtomicReference<SerializedEvent> lastSentEvent = new AtomicReference<>();
    private final Logger logger = LoggerFactory.getLogger(SequenceValidationStreamObserver.class);

    public SequenceValidationStreamObserver(
            StreamObserver<SerializedEvent> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void onNext(SerializedEvent event) {
        SerializedEvent prevEvent = lastSentEvent.get();
        if (prevEvent == null || prevEvent.getAggregateSequenceNumber() + 1 == event.getAggregateSequenceNumber()) {
            delegate.onNext(event);
            lastSentEvent.set(event);
        } else {
            String message = String.format("Invalid sequence number for aggregate %s. Received: %d, expected: %d",
                                           event.getAggregateIdentifier(),
                                           event.getAggregateSequenceNumber(),
                                           prevEvent.getAggregateSequenceNumber() + 1);
            logger.error(message);
            delegate.onError(new RuntimeException(message));
        }
    }

    @Override
    public void onError(Throwable throwable) {
        delegate.onError(throwable);
    }

    @Override
    public void onCompleted() {
        delegate.onCompleted();
    }
}
