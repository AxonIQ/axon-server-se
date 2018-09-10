package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axondb.Event;
import io.axoniq.axonserver.localstorage.EventInformation;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.transaction.PreparedTransaction;
import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Author: marc
 */
public class InputStreamEventStore extends SegmentBasedEventStore {
    private final SortedSet<Long> segments = new ConcurrentSkipListSet<>(Comparator.reverseOrder());
    private final EventTransformerFactory eventTransformerFactory;

    public InputStreamEventStore(EventTypeContext context, IndexManager indexManager,
                               EventTransformerFactory eventTransformerFactory,
                               StorageProperties storageProperties) {
        super(context, indexManager, storageProperties);
        this.eventTransformerFactory = eventTransformerFactory;
    }

    @Override
    protected void handover(Long segment, Runnable callback) {
        segments.add(segment);
        callback.run();
    }

    @Override
    protected void init(long lastInitialized) {
        segments.addAll(prepareSegmentStore(lastInitialized));
        if( next != null) next.init(segments.isEmpty() ? lastInitialized : segments.last());

    }

    @Override
    protected Optional<EventSource> getEventSource(long segment) {
        logger.warn("Get eventsource: {}", segment);
        InputStreamEventSource eventSource = get(segment);
        logger.warn("result={}", eventSource);
        if( eventSource == null)
            return Optional.empty();
        return Optional.of(eventSource);
    }

    @Override
    protected SortedSet<Long> getSegments() {
        return segments;
    }

    @Override
    protected SortedSet<PositionInfo> getPositions(long segment, String aggregateId) {
        return indexManager.getPositions(segment, aggregateId   );
    }

    @Override
    public PreparedTransaction prepareTransaction(List<Event> eventList) {
        throw new UnsupportedOperationException();
    }


    private InputStreamEventSource get(long segment) {
        if( ! segments.contains(segment)) return null;

        return new InputStreamEventSource(storageProperties.dataFile(context, segment), eventTransformerFactory, storageProperties);
    }

    @Override
    protected void recreateIndex(long segment) {
        try (InputStreamEventSource is = get(segment);
             EventIterator iterator = createEventIterator( is,segment, segment)) {
            Map<String, SortedSet<PositionInfo>> aggregatePositions = new HashMap<>();
            while (iterator.hasNext()) {
                EventInformation event = iterator.next();
                if (isDomainEvent(event.getEvent())) {
                    aggregatePositions.computeIfAbsent(event.getEvent().getAggregateIdentifier(),
                                                       k -> new ConcurrentSkipListSet<>())
                                      .add(new PositionInfo(event.getPosition(),
                                                            event.getEvent().getAggregateSequenceNumber()));
                }
            }
            indexManager.createIndex(segment, aggregatePositions, true);
        }

    }

}
