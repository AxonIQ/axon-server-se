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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Author: marc
 */
public abstract class SegmentBasedEventStore {
    protected static final Logger logger = LoggerFactory.getLogger(SegmentBasedEventStore.class);

    static final byte VERSION = 0;
    protected final String context;
    protected final IndexManager indexManager;
    protected final StorageProperties storageProperties;
    protected volatile SegmentBasedEventStore next;
    private final GroupContext type;

    public SegmentBasedEventStore(GroupContext eventTypeContext, IndexManager indexManager, StorageProperties storageProperties) {
        this.type = eventTypeContext;
        this.context = eventTypeContext.getContext();
        this.indexManager = indexManager;
        this.storageProperties = storageProperties;
    }


    public abstract void handover(Long segment, Runnable callback);

    public void next(SegmentBasedEventStore datafileManager) {
        SegmentBasedEventStore last = this;
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
        if( next != null) return next.getFirstToken();
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
                throw new LogException(ErrorCode.VALIDATION_FAILED, String.format("Validation exception: segment %d ending at %d", thisResult.getSegment(), thisResult.getLastToken()));
            }
        }
    }

    private Stream<Long> getAllSegments() {
        if( next == null) return getSegments().stream();
        return Stream.concat(getSegments().stream(), next.getSegments().stream()).distinct();
    }

    protected ValidationResult validateSegment(long segment) {
        logger.debug("{}: Validating {} segment: {}", type.getContext(), type.getGroupId(), segment);
        Iterator<Entry> iterator = getEntries(segment, segment, true);
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

    public GroupContext getType() {
        return type;
    }

    public abstract void initSegments(long maxValue);


    protected SegmentEntryIterator getEntries(long segment, long token, boolean validating) {
        Optional<EntrySource> reader = getEventSource(segment);
        return reader.map(r -> createIterator(r, segment, token, getPosition(segment, token), validating))
                     .orElseGet(() -> next.getEntries(segment, token, validating));
    }

    protected SegmentEntryIterator createIterator(EntrySource eventSource, long segment, long startIndex, int startPosition, boolean validating) {
        return eventSource.createEventIterator(segment, startIndex, startPosition, validating);
    }

    public long getSegmentFor(long token) {
        return getSegments().stream()
                            .filter( segment ->segment <= token)
                            .findFirst()
                            .orElse(next == null ? -1 : next.getSegmentFor(token));
    }

    public boolean replicated() {
        return true;
    }

    protected SortedSet<Long> prepareSegmentStore(long lastInitialized) {
        SortedSet<Long> segments = new ConcurrentSkipListSet<>(Comparator.reverseOrder());
        File events  = new File(storageProperties.getStorage(type));
        FileUtils.checkCreateDirectory(events);
        String[] eventFiles = FileUtils.getFilesWithSuffix(events, storageProperties.getLogSuffix());
        Arrays.stream(eventFiles)
              .map(name -> Long.valueOf(name.substring(0, name.indexOf('.'))))
              .filter(segment -> segment < lastInitialized)
              .forEach(segments::add);

        long firstValidIndex = segments.stream().filter(this::indexValid).findFirst().orElse(-1L);
        logger.debug("First valid index: {}", firstValidIndex);
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
        Stream<String> filenames = getSegments().stream().filter(s -> s > lastSegmentBackedUp).map( s -> Stream.of(
                storageProperties.logFile(type, s).getAbsolutePath(),
                storageProperties.indexFile(type, s).getAbsolutePath()
        )).flatMap(Function.identity());
        if( next == null) return filenames;
        return Stream.concat(filenames, next.getBackupFilenames(lastSegmentBackedUp));
    }

//    @Override
//    public void health(Health.Builder builder) {
//        String storage = storageProperties.getStorage(context);
//        SegmentBasedEventStore n = next;
//        Path path = Paths.get(storage);
//        try {
//            FileStore store = Files.getFileStore(path);
//            builder.withDetail(context + ".free",store.getUsableSpace());
//            builder.withDetail(context + ".path",path.toString());
//        } catch (IOException e) {
//            logger.warn("Failed to retrieve filestore for {}", path, e);
//        }
//        while( n != null) {
//            if( ! storage.equals(next.storageProperties.getStorage(context))) {
//                n.health(builder);
//                return;
//            }
//            n = n.next;
//        }
//    }


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

    public SegmentEntryIterator getIterator(long nextIndex) {
        long segment = getSegmentFor(nextIndex);
        Optional<EntrySource> eventSource = getEventSource(segment);
        if( eventSource.isPresent()) {
            return eventSource.get().createEventIterator(segment, nextIndex, getPosition(segment, nextIndex), false);
        }
        if( next != null) {
            return next.getIterator(nextIndex);
        }
        return null;
    }

    protected abstract int getPosition(long segment, long nextIndex);
}
