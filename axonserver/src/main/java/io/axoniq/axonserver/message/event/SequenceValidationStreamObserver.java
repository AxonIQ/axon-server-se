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
    private final SequenceValidationStrategy sequenceValidationStrategy;
    private final AtomicReference<SerializedEvent> lastSentEvent = new AtomicReference<>();
    private final String context;
    private final Logger logger = LoggerFactory.getLogger(SequenceValidationStreamObserver.class);

    public SequenceValidationStreamObserver(
            CallStreamObserver<SerializedEvent> delegate,
            SequenceValidationStrategy sequenceValidationStrategy,
            String context) {
        super(delegate);
        this.sequenceValidationStrategy = sequenceValidationStrategy;
        this.context = context;
    }

    @Override
    public void onNext(SerializedEvent event) {
        SerializedEvent prevEvent = lastSentEvent.get();
        if (prevEvent == null || prevEvent.getAggregateSequenceNumber() + 1 == event.getAggregateSequenceNumber()) {
            delegate().onNext(event);
            lastSentEvent.set(event);
        } else {
            String message = String.format("Invalid sequence number for aggregate %s in context %s. "
                                                   + "Received: %d, expected: %d",
                                           event.getAggregateIdentifier(),
                                           context,
                                           event.getAggregateSequenceNumber(),
                                           prevEvent.getAggregateSequenceNumber() + 1);
            if (SequenceValidationStrategy.FAIL.equals(sequenceValidationStrategy)) {
                logger.info(message);
                delegate().onError(new RuntimeException(message));
            } else {
                logger.warn(message);
                delegate().onNext(event);
                lastSentEvent.set(event);
            }
        }
    }
}
