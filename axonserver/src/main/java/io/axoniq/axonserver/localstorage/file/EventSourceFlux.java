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
    private final List<Integer> positions;
    private final EventSourceFactory eventSourceFactory;

    /**
     * Creates a new instance able to read events from the specified position using the provided {@link
     * EventSourceFactory}.
     *
     * @param positions          the list of the positions of the interesting events in the segment.
     * @param eventSourceFactory the factory used to open a new {@link EventSource} to access the segment file.
     */
    public EventSourceFlux(List<Integer> positions, EventSourceFactory eventSourceFactory) {
        this.positions = positions;
        this.eventSourceFactory = eventSourceFactory;
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
                    sink.complete();
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
                while (count < requested && nextPositionIndex.get() < positions.size()) {
                    logger.trace("Reading event from EventSource in the thread {}", Thread.currentThread().getName());
                    try {
                        SerializedEvent event = eventSource.readEvent(positions.get(nextPositionIndex
                                                                                            .getAndIncrement()));
                        count++;
                        sink.next(event);
                    } catch (Exception e) {
                        sink.error(e);
                        return;
                    }
                }

                if (nextPositionIndex.get() >= positions.size()) {
                    sink.complete();
                }
            });

            sink.onDispose(eventSource::close);
        });
    }
}
