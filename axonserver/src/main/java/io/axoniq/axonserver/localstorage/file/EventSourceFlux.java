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
 * @author Milan Savic
 */
public class EventSourceFlux implements Supplier<Flux<SerializedEvent>> {

    private static final Logger logger = LoggerFactory.getLogger(EventSourceFlux.class);
    private final List<Integer> positions;
    private final EventSourceFactory eventSourceFactory;

    public EventSourceFlux(List<Integer> positions, EventSourceFactory eventSourceFactory) {
        this.positions = positions;
        this.eventSourceFactory = eventSourceFactory;
    }

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
