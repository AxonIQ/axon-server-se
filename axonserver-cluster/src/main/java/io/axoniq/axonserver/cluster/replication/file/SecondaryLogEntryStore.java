package io.axoniq.axonserver.cluster.replication.file;

import io.axoniq.axonserver.cluster.exception.ErrorCode;
import io.axoniq.axonserver.cluster.exception.LogException;
import io.axoniq.axonserver.cluster.util.AxonThreadFactory;
import io.axoniq.axonserver.grpc.cluster.Entry;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.WeakReference;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * @author Marc Gathier
 * @since 4.1
 */
public class SecondaryLogEntryStore extends SegmentBasedLogEntryStore {

    private final ScheduledExecutorService scheduledExecutorService;
    private final SortedSet<Long> segments = new ConcurrentSkipListSet<>(Comparator.reverseOrder());
    private final ConcurrentSkipListMap<Long, WeakReference<ByteBufferEntrySource>> lruMap = new ConcurrentSkipListMap<>();
    private final LogEntryTransformerFactory logEntryTransformerFactory;


    public SecondaryLogEntryStore(String context, IndexManager indexManager,
                                  LogEntryTransformerFactory logEntryTransformerFactory,
                                  StorageProperties storageProperties) {
        super(context, indexManager, storageProperties);
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new AxonThreadFactory(
                context + "-file-cleanup-"));
        this.logEntryTransformerFactory = logEntryTransformerFactory;
    }


    @Override
    public void initSegments(long lastInitialized) {
        segments.addAll(prepareSegmentStore(lastInitialized));
        if (next != null) {
            next.initSegments(segments.isEmpty() ? lastInitialized : segments.last());
        }
    }

    protected void recreateIndex(long segment) {
        ByteBufferEntrySource buffer = get(segment, true);
        try (SegmentEntryIterator iterator = buffer.createLogEntryIterator(segment, segment, 5, false)) {
            Map<Long, Integer> aggregatePositions = new HashMap<>();
            while (iterator.hasNext()) {
                Entry event = iterator.next();
                aggregatePositions.put(event.getIndex(), iterator.startPosition());
            }
            indexManager.createIndex(segment, aggregatePositions, true);
        }
    }

    @Override
    protected Optional<EntrySource> getEventSource(long segment) {
        return Optional.ofNullable(get(segment, false));
    }

    @Override
    protected SortedSet<Long> getSegments() {
        return segments;
    }

    @Override
    public void handover(Long segment, Runnable callback) {
        segments.add(segment);
        if (next != null && segments.size() > storageProperties.getNumberOfSegments()) {
            segments.stream().skip(storageProperties.getNumberOfSegments()).forEach(s -> next.handover(s, () -> {
                segments.remove(s);
                indexManager.remove(s);
                WeakReference<ByteBufferEntrySource> fileRef = lruMap.remove(s);
                if (fileRef != null) {
                    ByteBufferEntrySource file = fileRef.get();
                    if (file != null) {
                        file.clean(storageProperties.getSecondaryCleanupDelay());
                    }
                }
                scheduledExecutorService.schedule(() -> deleteFiles(s), 20, TimeUnit.SECONDS);
            }));
        }
        callback.run();
    }

    private void deleteFiles(Long s) {
        logger.debug("{}: Deleting files for segment {}", context, s);
        File index = storageProperties.indexFile(context, s);
        File datafile = storageProperties.logFile(context, s);
        boolean success = FileUtils.delete(index) && FileUtils.delete(datafile);

        if (!success) {
            logger.warn("{}: Deleting files for segment {} not complete, rescheduling", context, s);
            scheduledExecutorService.schedule(() -> deleteFiles(s), 1, TimeUnit.MINUTES);
        }
    }

    @Override
    public void cleanup(int delay) {
        lruMap.forEach((s, source) -> {
            ByteBufferEntrySource eventSource = source.get();
            if (eventSource != null) {
                eventSource.clean(delay);
            }
        });
        indexManager.cleanup();
    }

    @Override
    public void rollback(long token) {
        for (long segment : getSegments()) {
            if (segment > token) {
                removeSegment(segment);
            }
        }

        if (segments.isEmpty() && next != null) {
            next.rollback(token);
        }
    }

    @Override
    protected Entry getEntry(long index) {
        long segment = getSegmentFor(index);
        if (segments.contains(segment)) {
            Integer position = indexManager.getIndex(segment).getPosition(index);
            EntrySource eventSource = getEventSource(segment).orElse(null);
            if (eventSource != null && position != null) {
                return eventSource.readLogEntry(position, index);
            }
        }
        return null;
    }

    @Override
    protected int getPosition(long segment, long nextIndex) {
        return indexManager.getIndex(segment).getPosition(nextIndex);
    }

    protected void removeSegment(long segment) {
        if (segments.remove(segment)) {
            WeakReference<ByteBufferEntrySource> segmentRef = lruMap.remove(segment);
            if (segmentRef != null) {
                ByteBufferEntrySource eventSource = segmentRef.get();
                if (eventSource != null) {
                    eventSource.clean(0);
                }
            }

            indexManager.remove(segment);
            deleteFiles(segment);
        }
    }

    @Override
    protected void clearOlderThan(long time, TimeUnit timeUnit, LongSupplier lastAppliedIndexSupplier) {
        long filter = System.currentTimeMillis() - timeUnit.toMillis(time);
        long lastAppliedIndexSegment = segmentContainingLastAppliedIndex(segments,
                                                                         lastAppliedIndexSupplier.getAsLong());
        List<Long> segmentsToBeDeleted = segments.stream()
                                                 .filter(segment -> applied(segment, lastAppliedIndexSegment))
                                                 .filter(segment -> olderThan(segment,
                                                                              filter)) //f.lastModified() <= filter) // filter out files older than <time>
                                                 .collect(Collectors.toList());
        String formattedSegmentsToBeDeleted = segmentsToBeDeleted.stream()
                                                                 .map(Object::toString)
                                                                 .collect(Collectors.joining(","));
        if (!segmentsToBeDeleted.isEmpty()) {
            logger.info("Deleting segments {}.", formattedSegmentsToBeDeleted);
        } else {
            logger.debug("Deleting segments {}.", formattedSegmentsToBeDeleted);
        }

        segmentsToBeDeleted.forEach(this::removeSegment);
        logger.debug("Segments deleted {}.", formattedSegmentsToBeDeleted);
    }

    private boolean olderThan(Long segment, long filter) {
        File segmentFile = storageProperties.logFile(context, segment);
        return segmentFile.lastModified() <= filter;
    }

    private long segmentContainingLastAppliedIndex(Collection<Long> segments, long lastAppliedIndex) {
        return segments.stream()
                       .filter(segment -> segment.compareTo(lastAppliedIndex) <= 0)
                       .sorted()
                       .max(Long::compareTo)
                       .orElse(1L);
    }

    private boolean applied(long segment, long segmentContainingLastAppliedIndex) {
        return segment < segmentContainingLastAppliedIndex;
    }


    private ByteBufferEntrySource get(long segment, boolean force) {
        if (!segments.contains(segment) && !force) {
            return null;
        }
        WeakReference<ByteBufferEntrySource> bufferRef = lruMap.get(segment);
        if (bufferRef != null) {
            ByteBufferEntrySource b = bufferRef.get();
            if (b != null) {
                return b.duplicate();
            }
        }

        File file = storageProperties.logFile(context, segment);
        long size = file.length();

        try (FileChannel fileChannel = new RandomAccessFile(file, "r").getChannel()) {
            MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, size);
            ByteBufferEntrySource eventSource = new ByteBufferEntrySource(buffer,
                                                                          logEntryTransformerFactory,
                                                                          storageProperties);
            lruMap.put(segment, new WeakReference<>(eventSource));
            return eventSource;
        } catch (IOException ioException) {
            throw new LogException(ErrorCode.DATAFILE_READ_ERROR,
                                   "Error while opening segment: " + segment,
                                   ioException);
        }
    }
}
