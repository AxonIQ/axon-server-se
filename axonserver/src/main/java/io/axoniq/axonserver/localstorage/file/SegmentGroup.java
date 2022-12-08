package io.axoniq.axonserver.localstorage.file;

import java.util.Optional;
import java.util.SortedSet;
import java.util.stream.Stream;

/**
 * @author Stefan Dragisic
 */
public interface SegmentGroup {

    void handover(Segment segment, Runnable callback);

    SortedSet<Long> getSegments();

    long getFirstToken();

    long getTokenAt(long instant);

    long getSegmentFor(long token);

    Stream<String> getBackupFilenames(long lastSegmentBackedUp);

    Optional<EventSource> eventSource(long segment);

    void initSegments(long l);

    void close(boolean deleteData);

    long getFirstCompletedSegment();

    Stream<Long> getAllSegments();
}
