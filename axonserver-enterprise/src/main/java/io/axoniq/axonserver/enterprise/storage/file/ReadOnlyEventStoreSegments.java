package io.axoniq.axonserver.enterprise.storage.file;

import io.axoniq.axonserver.enterprise.storage.multitier.MultiTierInformationProvider;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.localstorage.EventType;
import io.axoniq.axonserver.localstorage.EventTypeContext;
import io.axoniq.axonserver.localstorage.file.ByteBufferEventSource;
import io.axoniq.axonserver.localstorage.file.EventByteBufferIterator;
import io.axoniq.axonserver.localstorage.file.EventIterator;
import io.axoniq.axonserver.localstorage.file.EventSource;
import io.axoniq.axonserver.localstorage.file.FileUtils;
import io.axoniq.axonserver.localstorage.file.IndexManager;
import io.axoniq.axonserver.localstorage.file.SegmentBasedEventStore;
import io.axoniq.axonserver.localstorage.file.StorageProperties;
import io.axoniq.axonserver.localstorage.transformation.EventTransformerFactory;
import io.axoniq.axonserver.metric.BaseMetricName;
import io.axoniq.axonserver.metric.MeterFactory;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.WeakReference;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Comparator;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Manages read only segments for reading older events. When a segment is completed and the indexes are written, it is
 * handed over to this manager. There is one instance per context for events and one for snapshots.
 *
 * @author Marc Gathier
 * @since 4.4
 */
public class ReadOnlyEventStoreSegments extends SegmentBasedEventStore {

    private final ScheduledExecutorService scheduledExecutorService;
    private final SortedSet<Long> segments = new ConcurrentSkipListSet<>(Comparator.reverseOrder());
    private final ConcurrentSkipListMap<Long, WeakReference<ByteBufferEventSource>> lruMap = new ConcurrentSkipListMap<>();
    private final EventTransformerFactory eventTransformerFactory;
    private final MultiTierInformationProvider multiTierInformationProvider;
    private final long deleteDelay;

    public ReadOnlyEventStoreSegments(EventTypeContext context, IndexManager indexManager,
                                      EventTransformerFactory eventTransformerFactory,
                                      StorageProperties storageProperties,
                                      MultiTierInformationProvider multiTierInformationProvider,
                                      MeterFactory meterFactory) {
        this(context, indexManager, eventTransformerFactory,
             storageProperties, multiTierInformationProvider, meterFactory, TimeUnit.SECONDS.toMillis(15));
    }

    public ReadOnlyEventStoreSegments(EventTypeContext context, IndexManager indexManager,
                                      EventTransformerFactory eventTransformerFactory,
                                      StorageProperties storageProperties,
                                      MultiTierInformationProvider multiTierInformationProvider,
                                      MeterFactory meterFactory,
                                      long deleteDelay) {
        super(context, indexManager, storageProperties, meterFactory);
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new CustomizableThreadFactory(
                context + "-file-close-"));
        this.eventTransformerFactory = eventTransformerFactory;
        this.multiTierInformationProvider = multiTierInformationProvider;
        this.deleteDelay = deleteDelay;
    }

    @Override
    public void initSegments(long lastInitialized) {
        segments.addAll(prepareSegmentStore(lastInitialized));

        if (next != null) {
            next.initSegments(segments.isEmpty() ? lastInitialized : segments.last());
        }
    }

    protected void recreateIndex(long segment) {
        logger.warn("{}: recreate index for {}", type, segment);
        ByteBufferEventSource buffer = get(segment, true);
        EventIterator iterator = new EventByteBufferIterator(buffer, segment, segment);
        recreateIndexFromIterator(segment, iterator);
    }

    @Override
    public Optional<EventSource> getEventSource(long segment) {
        return Optional.ofNullable(get(segment, false));
    }

    @Override
    protected SortedSet<Long> getSegments() {
        return segments;
    }

    /**
     * Receives a new segment to manage. If this context is multi-tier and there are next level tiers, it checks if it
     * can delete older segments. Older segments can be removed if all nodes in the next tier have stored the
     * information.
     *
     * @param segment  the base filename for the segment
     * @param callback callback to execute when done
     */
    @Override
    public void handover(Long segment, Runnable callback) {
        segments.add(segment);
        if (multiTierInformationProvider.isMultiTier(context)) {
            long minTimestamp = System.currentTimeMillis() - storageProperties.getRetentionTime(
                    multiTierInformationProvider.tier(context));
            long minTokenAtSecondaryNodes = multiTierInformationProvider.safeToken(context,
                                                                                   isEvent() ? BaseMetricName.AXON_EVENT_LAST_TOKEN : BaseMetricName.AXON_SNAPSHOT_LAST_TOKEN);
            long nextSegment = Long.MAX_VALUE;
            SortedSet<Long> segmentsToDelete = new TreeSet<>();
            for (Long candidate : segments) {
                if (candidate < minTokenAtSecondaryNodes
                        && nextSegment < minTokenAtSecondaryNodes
                        && storageProperties.dataFile(context, candidate).lastModified() < minTimestamp) {
                    segmentsToDelete.add(candidate);
                }
                nextSegment = candidate;
            }

            segmentsToDelete.forEach(segments::remove);
            scheduledExecutorService.schedule(() -> deleteFiles(segmentsToDelete), deleteDelay, TimeUnit.MILLISECONDS);
        }
        callback.run();
    }

    private boolean isEvent() {
        return EventType.EVENT.equals(getType().getEventType());
    }

    private void deleteFiles(SortedSet<Long> segments) {
        boolean success = true;
        while (success && !segments.isEmpty()) {
            long s = segments.first();
            logger.debug("Deleting {} files for segment {}", getType().getEventType(), s);
            WeakReference<ByteBufferEventSource> reference = lruMap.remove(s);
            if (reference != null) {
                ByteBufferEventSource source = reference.get();
                if (source != null) {
                    source.clean(0);
                }
            }
            File datafile = storageProperties.dataFile(context, s);
            success = indexManager.remove(s) && FileUtils.delete(datafile);
            if (success) {
                segments.remove(s);
            }
        }

        if (!success) {
            logger.warn("{}: Deleting {} files for segments {} not complete, rescheduling",
                        context,
                        getType().getEventType(),
                        segments);
            scheduledExecutorService.schedule(() -> deleteFiles(segments), 1, TimeUnit.MINUTES);
        }
    }

    @Override
    public void close(boolean deleteData) {
        lruMap.forEach((s, source) -> {
            ByteBufferEventSource eventSource = source.get();
            if (eventSource != null) {
                eventSource.clean(0);
            }
        });
        lruMap.clear();
        if (deleteData) {
            segments.forEach(this::removeSegment);
            segments.clear();
        }
    }

    @Override
    public void rollback(long token) {
        segments.forEach(s -> {
            if (s > token) {
                removeSegment(s);
            }
        });
        segments.removeIf(s -> s > token);
        if (segments.isEmpty() && next != null) {
            next.rollback(token);
        }
    }

    @Override
    public void deleteAllEventData() {
        throw new UnsupportedOperationException("Development mode deletion is not supported in clustered environments");
    }

    private void removeSegment(long segment) {
        WeakReference<ByteBufferEventSource> segmentRef = lruMap.remove(segment);
        if (segmentRef != null) {
            ByteBufferEventSource eventSource = segmentRef.get();
            if (eventSource != null) {
                eventSource.clean(0);
            }
        }

        indexManager.remove(segment);
        FileUtils.delete(storageProperties.dataFile(context, segment));
    }

    private ByteBufferEventSource get(long segment, boolean force) {
        if (!segments.contains(segment) && !force) {
            return null;
        }
        WeakReference<ByteBufferEventSource> bufferRef = lruMap.get(segment);
        if (bufferRef != null) {
            ByteBufferEventSource b = bufferRef.get();
            if (b != null) {
                return b.duplicate();
            }
        }

        File file = storageProperties.dataFile(context, segment);
        long size = file.length();

        try (FileChannel fileChannel = new RandomAccessFile(file, "r").getChannel()) {
            MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, size);
            ByteBufferEventSource eventSource = new ByteBufferEventSource(file.getAbsolutePath(),
                                                                          buffer,
                                                                          eventTransformerFactory,
                                                                          storageProperties);
            lruMap.put(segment, new WeakReference<>(eventSource));
            return eventSource;
        } catch (IOException ioException) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR,
                                                 "Error while opening segment: " + segment,
                                                 ioException);
        }
    }
}
