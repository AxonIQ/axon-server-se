package io.axoniq.axonserver.cluster.replication.file;

import io.axoniq.axonserver.cluster.exception.ErrorCode;
import io.axoniq.axonserver.cluster.exception.LogException;
import io.axoniq.axonserver.grpc.cluster.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Marc Gathier
 * @since 4.1
 */
public abstract class SegmentBasedLogEntryStore {
    protected static final Logger logger = LoggerFactory.getLogger(SegmentBasedLogEntryStore.class);

    static final byte VERSION = 0;
    protected final String context;
    protected final IndexManager indexManager;
    protected final StorageProperties storageProperties;
    protected volatile SegmentBasedLogEntryStore next;

    public SegmentBasedLogEntryStore(String context, IndexManager indexManager, StorageProperties storageProperties) {
        this.context = context;
        this.indexManager = indexManager;
        this.storageProperties = storageProperties;
    }


    public abstract void handover(Long segment, Runnable callback);

    public void next(SegmentBasedLogEntryStore datafileManager) {
        SegmentBasedLogEntryStore last = this;
        while(last.next != null) {
            last = last.next;
        }
        last.next = datafileManager;
    }


    public void init(boolean validate) {
        initSegments(Long.MAX_VALUE);
        if( validate) validate(storageProperties.getValidationSegments());
    }

    public long getFirstToken() {
        if( next != null && !next.getSegments().isEmpty()) return next.getFirstToken();
        if( getSegments().isEmpty() ) return 0;
        return getSegments().last();
    }

    public void validate(int maxSegments) {
        Stream<Long> segments = getAllSegments();
        List<ValidationResult> resultList = segments.limit(maxSegments).parallel().map(this::validateSegment).collect(Collectors.toList());
        resultList.stream().filter(validationResult ->  !validationResult.isValid()).findFirst().ifPresent(validationResult -> {
            throw new LogException(ErrorCode.VALIDATION_FAILED, validationResult.getMessage());
        });
        resultList.sort(Comparator.comparingLong(ValidationResult::getSegment));
        for( int i = 0 ; i < resultList.size() - 1 ; i++) {
            ValidationResult thisResult = resultList.get(i);
            ValidationResult nextResult = resultList.get(i+1);
            if( thisResult.getLastToken() != nextResult.getSegment()) {
                throw new LogException(ErrorCode.VALIDATION_FAILED,
                                       String.format("%s: Validation exception: segment %d ending at %d",
                                                     context,
                                                     thisResult.getSegment(),
                                                     thisResult.getLastToken()));
            }
        }
    }

    private Stream<Long> getAllSegments() {
        if( next == null) return getSegments().stream();
        return Stream.concat(getSegments().stream(), next.getSegments().stream()).distinct();
    }

    protected ValidationResult validateSegment(long segment) {
        logger.debug("{}: Validating segment: {}", context, segment);
        Iterator<Entry> iterator = getEntries(segment, segment);
        try {
            Entry last = null;
            while (iterator.hasNext()) {
                last = iterator.next();
            }
            return new ValidationResult(segment, last == null ? segment : last.getIndex() + 1);
        } catch (Exception ex) {
            return new ValidationResult(segment, ex.getMessage());
        }
    }

    public abstract void initSegments(long maxValue);


    protected SegmentEntryIterator getEntries(long segment, long token) {
        Optional<EntrySource> reader = getEventSource(segment);
        return reader.map(r -> createIterator(r, token, getPosition(segment, token)))
                     .orElseGet(() -> next.getEntries(segment, token));
    }

    protected SegmentEntryIterator createIterator(EntrySource eventSource, long startIndex,
                                                  int startPosition) {
        return eventSource.createLogEntryIterator(startIndex, startPosition);
    }

    public long getSegmentFor(long token) {
        return getSegments().stream()
                            .filter( segment ->segment <= token)
                            .findFirst()
                            .orElse(next == null ? -1 : next.getSegmentFor(token));
    }

    protected SortedSet<Long> prepareSegmentStore(long lastInitialized) {
        SortedSet<Long> segments = new ConcurrentSkipListSet<>(Comparator.reverseOrder());
        File logEntryFolder = new File(storageProperties.getStorage(context));
        FileUtils.checkCreateDirectory(logEntryFolder);
        String[] entriesFiles = FileUtils.getFilesWithSuffix(logEntryFolder, storageProperties.getLogSuffix());
        Arrays.stream(entriesFiles)
              .map(name -> Long.valueOf(name.substring(0, name.indexOf('.'))))
              .filter(segment -> segment < lastInitialized)
              .forEach(segments::add);

        long firstValidIndex = segments.stream().filter(this::indexValid).findFirst().orElse(-1L);
        logger.debug("{}: First valid index: {}", context, firstValidIndex);
        return segments;
    }

    private boolean indexValid(long segment) {
        if( indexManager.validIndex(segment)) {
            return true;
        }

        recreateIndex(segment);
        return false;
    }

    protected abstract void recreateIndex(long segment);
    public abstract void cleanup(int delay);

    public abstract void rollback(long token);

    public Stream<String> getBackupFilenames(long lastSegmentBackedUp) {
        Stream<String> filenames = getSegments().stream().filter(s -> s > lastSegmentBackedUp).flatMap(s -> Stream.of(
                storageProperties.logFile(context, s).getAbsolutePath(),
                storageProperties.indexFile(context, s).getAbsolutePath()
        ));
        if( next == null) return filenames;
        return Stream.concat(filenames, next.getBackupFilenames(lastSegmentBackedUp));
    }

    /**
     * @param segment gets an EntrySource for the segment
     * @return the event source or Optional.empty() if segment not managed by this handler
     */
    protected abstract Optional<EntrySource> getEventSource(long segment);

    /**
     * Get all segments
     * @return descending set of segment ids
     */
    protected abstract SortedSet<Long> getSegments();

    protected abstract Entry getEntry(long index);

    public abstract long getTerm(long index);

    /**
     * Returns an iterator for the entries in the segment containing {@code nextIndex}.
     * Returns {@code null} when no segment for index found.
     *
     * @param nextIndex index of first entry to return
     * @return the iterator
     */
    public SegmentEntryIterator getSegmentIterator(long nextIndex) {
        long segment = getSegmentFor(nextIndex);
        Optional<EntrySource> eventSource = getEventSource(segment);
        if( eventSource.isPresent()) {
            return eventSource.get().createLogEntryIterator(nextIndex, getPosition(segment, nextIndex));
        }
        if( next != null) {
            return next.getSegmentIterator(nextIndex);
        }
        return null;
    }

    protected abstract int getPosition(long segment, long nextIndex);

    protected abstract void removeSegment(long segment);

    protected abstract void clearOlderThan(long time, TimeUnit timeUnit, LongSupplier lastAppliedIndexSupplier);

    /**
     * Confirms that store is gracefully closed
     */
    public abstract boolean isClosed();

    /**
     * Gracefully closes all thread executors that are active
     * @param deleteData cleans up and removes store data
     */
    public abstract void close(boolean deleteData);
}
