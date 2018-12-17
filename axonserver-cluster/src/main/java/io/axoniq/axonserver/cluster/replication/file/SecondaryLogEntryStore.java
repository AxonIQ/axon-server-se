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
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Author: marc
 */
public class SecondaryLogEntryStore extends SegmentBasedLogEntryStore {
    private final ScheduledExecutorService scheduledExecutorService;
    private final SortedSet<Long> segments = new ConcurrentSkipListSet<>(Comparator.reverseOrder());
    private final ConcurrentSkipListMap<Long, WeakReference<ByteBufferEntrySource>> lruMap = new ConcurrentSkipListMap<>();
    private final LogEntryTransformerFactory eventTransformerFactory;


    public SecondaryLogEntryStore(String context, IndexManager indexManager,
                                  LogEntryTransformerFactory eventTransformerFactory,
                                  StorageProperties storageProperties) {
        super(context, indexManager, storageProperties);
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new AxonThreadFactory(context + "-file-cleanup-"));
        this.eventTransformerFactory = eventTransformerFactory;
    }


    @Override
    public void initSegments(long lastInitialized)  {
        segments.addAll(prepareSegmentStore(lastInitialized));
        if( next != null) next.initSegments(segments.isEmpty() ? lastInitialized : segments.last());
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
        if( next != null && segments.size() > storageProperties.getNumberOfSegments()) {
            segments.stream().skip(storageProperties.getNumberOfSegments()).forEach(s -> next.handover(s, () -> {
                segments.remove(s);
                indexManager.remove(s);
                WeakReference<ByteBufferEntrySource> fileRef = lruMap.remove(s);
                if( fileRef != null) {
                    ByteBufferEntrySource file = fileRef.get();
                    if( file != null) {
                        file.clean(storageProperties.getSecondaryCleanupDelay());
                    }
                }
                scheduledExecutorService.schedule(()-> deleteFiles(s), 20, TimeUnit.SECONDS);
            }));
        }
        callback.run();
    }

    private void deleteFiles(Long s) {
        logger.debug("Deleting {} files for segment {}", getType(), s);
        File index = storageProperties.indexFile(getType(), s);
        File datafile = storageProperties.logFile(getType(), s);
        boolean success = FileUtils.delete(index) && FileUtils.delete(datafile);

        if( ! success) {
            logger.debug("Deleting {} files for segment {} not complete, rescheduling", getType(), s);
            scheduledExecutorService.schedule(()-> deleteFiles(s), 1, TimeUnit.MINUTES);
        }
    }

    @Override
    public void cleanup(int delay) {
        lruMap.forEach((s, source) -> {
            ByteBufferEntrySource eventSource = source.get();
            if( eventSource != null) {
                eventSource.clean(delay);
            }
        });
        indexManager.cleanup();
    }

    @Override
    public void rollback( long token) {
        for( long segment: getSegments()) {
            if( segment > token) {
                removeSegment(segment);
            }
        }

        if( segments.isEmpty() && next != null) {
            next.rollback(token);
        }
    }

    @Override
    protected Entry getEntry(long index) {
        long segment = getSegmentFor(index);
        if( segments.contains(segment)) {
            Integer position = indexManager.getIndex(segment).getPosition(index);
            EntrySource eventSource = getEventSource(segment).orElse(null);
            if( eventSource != null && position != null) {
                return eventSource.readLogEntry(position, index);
            }
        }
        return null;
    }

    @Override
    protected int getPosition(long segment, long nextIndex) {
        return indexManager.getIndex(segment).getPosition(nextIndex);
    }

    private void removeSegment(long segment) {
        if( segments.remove(segment)) {
            WeakReference<ByteBufferEntrySource> segmentRef = lruMap.remove(segment);
            if (segmentRef != null) {
                ByteBufferEntrySource eventSource = segmentRef.get();
                if (eventSource != null) {
                    eventSource.clean(0);
                }
            }

            indexManager.remove(segment);
            FileUtils.delete(storageProperties.logFile(getType(), segment));
            FileUtils.delete(storageProperties.indexFile(getType(), segment));
        }
    }


    private ByteBufferEntrySource get(long segment, boolean force)  {
        if( ! segments.contains(segment) && !force) return null;
        WeakReference<ByteBufferEntrySource> bufferRef = lruMap.get(segment);
        if( bufferRef != null ) {
            ByteBufferEntrySource b =  bufferRef.get();
            if( b != null) {
                return b.duplicate();
            }
        }

        File file = storageProperties.logFile(getType(), segment);
        long size = file.length();

        try(FileChannel fileChannel = new RandomAccessFile(file, "r").getChannel()) {
            MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, size);
            ByteBufferEntrySource eventSource = new ByteBufferEntrySource(buffer, eventTransformerFactory, storageProperties);
            lruMap.put(segment, new WeakReference<>(eventSource));
            return eventSource;
        } catch (IOException ioException) {
            throw new LogException(ErrorCode.DATAFILE_READ_ERROR, "Error while opening segment: " + segment, ioException);
        }
    }

}
