package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.localstorage.SerializedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * This class is able to provide a {@link Flux<SerializedEvent>} that is possible to use in order to read the events
 * in a reactive way reading in the specified positions of segment using the provided {@link EventSourceFactory}.
 *
 * @author Milan Savic
 * @author Sara Pellegrini
 * @since 4.5
 */
public class EventSourceFlux implements Supplier<Flux<SerializedEvent>> {

    private static final Logger logger = LoggerFactory.getLogger(EventSourceFlux.class);
    private final IndexEntries indexEntries;
    private final EventSourceFactory eventSourceFactory;
    private final long segment;

    /**
     * Creates a new instance able to read events from the specified position using the provided {@link
     * EventSourceFactory}.
     *
     * @param indexEntries       the list of the positions of the interesting events in the segment.
     * @param eventSourceFactory the factory used to open a new {@link EventSource} to access the segment file.
     */
    public EventSourceFlux(IndexEntries indexEntries, EventSourceFactory eventSourceFactory, long segment) {
        this.indexEntries = indexEntries;
        this.eventSourceFactory = eventSourceFactory;
        this.segment = segment;
    }

    /**
     * Returns the {@link Flux<SerializedEvent>} that provides the events in the specified positions.
     *
     * @return the {@link Flux<SerializedEvent>} that provides the events in the specified positions.
     */
    @Override
    public Flux<SerializedEvent> get() {
        return Flux.create(sink -> {
            EventSource eventSource;
            try {
                Optional<EventSource> optional = eventSourceFactory.create();
                if (!optional.isPresent()) {
                    logger.warn("Event source not found for segment {}",segment);
                    sink.error(new EventSourceNotFoundException());
                    return;
                }
                eventSource = optional.get();
            } catch (Exception e) {
                sink.error(e);
                return;
            }
            final AtomicInteger nextPositionIndex = new AtomicInteger(0);

            sink.onRequest(requested -> {
                int count = 0;
                List<Integer> positions = indexEntries.positions();
                while (count < requested && nextPositionIndex.get() < indexEntries.size()) {

                    try {
                        SerializedEvent event = eventSource.readEvent(positions.get(nextPositionIndex
                                                                                            .getAndIncrement()));
                        logger.trace("Reading from EventSource the event with sequence number {} for aggregate {}",
                                     event.getAggregateSequenceNumber(),
                                     event.getAggregateIdentifier());

                        count++;
                        sink.next(event);
                    } catch (Exception e) {
                        sink.error(e);
                        return;
                    }
                }

                if (nextPositionIndex.get() >= indexEntries.size()) {
                    sink.complete();
                }
            });

            sink.onDispose(eventSource::close);
        });
    }
}
