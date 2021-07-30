package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.localstorage.DataFetcherSchedulerProvider;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import javax.annotation.Nonnull;

/**
 * This class is able to provide a {@link Flux<SerializedEvent>} that is possible to use in order to read the events in
 * a reactive way reading in the specified positions of segment using the provided {@link EventSourceFactory}.
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
    private final Supplier<ExecutorService> dataFetcherSchedulerProvider;
    private final int prefetch;

    /**
     * Creates a new instance able to read events from the specified position using the provided {@link
     * EventSourceFactory}.
     *
     * @param indexEntries       the list of the positions of the interesting events in the segment.
     * @param eventSourceFactory the factory used to open a new {@link EventSource} to access the segment file.
     */
    public EventSourceFlux(IndexEntries indexEntries, EventSourceFactory eventSourceFactory, long segment, int prefetch) {
        this(indexEntries, eventSourceFactory, segment, new DataFetcherSchedulerProvider(), prefetch);
    }


    /**
     * Creates a new instance able to read events from the specified position using the provided {@link
     * EventSourceFactory}.
     *
     * @param indexEntries       the list of the positions of the interesting events in the segment.
     * @param eventSourceFactory the factory used to open a new {@link EventSource} to access the segment file.
     */
    public EventSourceFlux(IndexEntries indexEntries,
                           EventSourceFactory eventSourceFactory,
                           long segment,
                           Supplier<ExecutorService> dataFetcherScheduler, int prefetch) {
        this.indexEntries = indexEntries;
        this.eventSourceFactory = eventSourceFactory;
        this.segment = segment;
        this.dataFetcherSchedulerProvider = dataFetcherScheduler;
        this.prefetch = prefetch;
    }


    /**
     * Returns the {@link Flux<SerializedEvent>} that provides the events in the specified positions.
     *
     * @return the {@link Flux<SerializedEvent>} that provides the events in the specified positions.
     */
    @Override
    public Flux<SerializedEvent> get() {
        return Flux.using(this::openEventSource,
                          eventSource -> Flux.just(eventSource)
                                             .flatMap(es -> Flux.fromIterable(indexEntries.positions())
                                                                .limitRate(prefetch, prefetch / 2)
                                                                .publishOn(Schedulers.fromExecutorService(dataFetcherSchedulerProvider.get()))
                                                                .map(es::readEvent))
                , EventSource::close);
    }

    @Nonnull
    private EventSource openEventSource() throws EventSourceNotFoundException {
        Optional<EventSource> optional = eventSourceFactory.create();
        if (!optional.isPresent()) {
            logger.warn("Event source not found for segment {}", segment);
           throw new EventSourceNotFoundException();
        }
        return optional.get();
    }

}


