package io.axoniq.axonserver.message.event;

import io.axoniq.axonserver.grpc.stream.CallStreamObserverDelegator;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.grpc.stub.CallStreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Stream observer wrapper that checks validity of aggregate sequence numbers.
 *
 * @author Sara Pellegrini
 * @since 4.4.11
 */
public class SequenceValidationStreamObserver extends CallStreamObserverDelegator<SerializedEvent> {

    private final AtomicReference<SerializedEvent> lastSentEvent = new AtomicReference<>();
    private final Logger logger = LoggerFactory.getLogger(SequenceValidationStreamObserver.class);

    public SequenceValidationStreamObserver(
            CallStreamObserver<SerializedEvent> delegate) {
        super(delegate);
    }

    @Override
    public void onNext(SerializedEvent event) {
        SerializedEvent prevEvent = lastSentEvent.get();
        if (prevEvent == null || prevEvent.getAggregateSequenceNumber() + 1 == event.getAggregateSequenceNumber()) {
            delegate().onNext(event);
            lastSentEvent.set(event);
        } else {
            String message = String.format("Invalid sequence number for aggregate %s. Received: %d, expected: %d",
                                           event.getAggregateIdentifier(),
                                           event.getAggregateSequenceNumber(),
                                           prevEvent.getAggregateSequenceNumber() + 1);
            delegate().onError(new RuntimeException(message));
            throw new RuntimeException(message);
        }
    }
}
