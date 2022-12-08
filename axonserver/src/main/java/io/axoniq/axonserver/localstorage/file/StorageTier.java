package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.localstorage.QueryOptions;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Optional;
import java.util.SortedSet;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * @author Stefan Dragisic
 */
public interface StorageTier {
    Flux<SerializedEvent>  eventsForPositions(long segment, IndexEntries indexEntries, int prefetch);

    void query(QueryOptions queryOptions, Predicate<EventWithToken> consumer);

    Optional<SerializedEvent> readSerializedEvent(long minSequenceNumber, long maxSequenceNumber, SegmentIndexEntries lastEventPosition);

    SortedSet<Long> getSegments();

    long getFirstToken();

    long getTokenAt(long instant);

    EventIterator getEvents(long segment, long token);

    TransactionIterator getTransactions(long segment, long token, boolean validating);

    long getSegmentFor(long token);

    int retrieveEventsForAnAggregate(long segment, List<Integer> indexEntries, long minSequenceNumber,
                                     long maxSequenceNumber,
                                     Consumer<SerializedEvent> onEvent, long maxResults, long minToken);

    Stream<String> getBackupFilenames(long lastSegmentBackedUp, boolean includeActive);

    Optional<EventSource> eventSource(long segment);

    void close(boolean deleteData);

    void initSegments(long first);

    long getFirstCompletedSegment();

    void handover(Segment segment, Runnable callback);
}
