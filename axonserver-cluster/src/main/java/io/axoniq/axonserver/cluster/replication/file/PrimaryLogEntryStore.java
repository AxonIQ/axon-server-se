package io.axoniq.axonserver.cluster.replication.file;

import io.axoniq.axonserver.cluster.exception.ErrorCode;
import io.axoniq.axonserver.cluster.exception.LogException;
import io.axoniq.axonserver.cluster.replication.EntryIterator;
import io.axoniq.axonserver.grpc.cluster.Entry;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

/**
 * @author Marc Gathier
 * @since 4.1
 */
public class PrimaryLogEntryStore extends SegmentBasedLogEntryStore {
    private static final Logger logger = LoggerFactory.getLogger(PrimaryLogEntryStore.class);
    private static final int HEADER_BYTES = 4 + 1 + 8 + 4; // Entry Size + Version byte + Term + Log Entry Type
    private static final int TX_CHECKSUM_BYTES = 4;

    private final LogEntryTransformerFactory logEntryTransformerFactory;
    private final Synchronizer synchronizer;
    private final AtomicReference<WritePosition> writePositionRef = new AtomicReference<>();
    private final AtomicLong lastToken = new AtomicLong(0);
    private final ConcurrentNavigableMap<Long, Map<Long, Integer>> positionsPerSegmentMap = new ConcurrentSkipListMap<>();
    private final Map<Long, ByteBufferEntrySource> readBuffers = new ConcurrentHashMap<>();
    private LogEntryTransformer logEntryTransformer;

    public PrimaryLogEntryStore(String context, IndexManager indexCreator, LogEntryTransformerFactory logEntryTransformerFactory, StorageProperties storageProperties) {
        super(context, indexCreator, storageProperties);
        this.logEntryTransformerFactory = logEntryTransformerFactory;
        synchronizer = new Synchronizer(context, storageProperties, this::completeSegment);
    }

    @Override
    public void initSegments(long lastInitialized) {
        logger.info("{}: Initializing log", context);
        File storageDir = new File(storageProperties.getStorage(context));
        FileUtils.checkCreateDirectory(storageDir);
        logEntryTransformer = logEntryTransformerFactory.get(VERSION, storageProperties.getFlags(), storageProperties);
        initLatestSegment(lastInitialized, Long.MAX_VALUE, storageDir, false);
    }

    public CompletableFuture<Long> write(long term, int type, byte[] bytes) {
        if (bytes.length == 0) throw new LogException(ErrorCode.VALIDATION_FAILED, "Cannot store empty log entry");
        PreparedTransaction preparedTransaction = prepareTransaction(bytes);
        return store(term, type, preparedTransaction);
    }

    @Override
    public SegmentEntryIterator getSegmentIterator(long nextIndex) {
        if (nextIndex > lastToken.get()) return null;
        logger.trace("{}: Create iterator at: {}", context, nextIndex);
        return super.getSegmentIterator(nextIndex);
    }

    public EntryIterator getEntryIterator(long nextIndex) {
        return new MultiSegmentIterator(this::getSegmentIterator, this::getLastToken, nextIndex);
    }

    public Entry getEntry(long index) {
        if (index > lastToken.get()) return null;
        long segment = getSegmentFor(index);
        ByteBufferEntrySource entrySource = readBuffers.get(segment);
        if (entrySource != null) {
            Integer position = positionsPerSegmentMap.get(segment).get(index);
            if (position == null) {
                return null;
            }
            return entrySource.duplicate().readLogEntry(position, index);
        }
        if (next != null) {
            return next.getEntry(index);
        }
        return null;
    }

    @Override
    protected int getPosition(long segment, long nextIndex) {
        try {
            return positionsPerSegmentMap.get(segment).get(nextIndex);
        } catch (NullPointerException npe) {
            throw new LogException(ErrorCode.DATAFILE_READ_ERROR,
                                   String.format("Null in get position %d lastIndex %d", nextIndex, lastToken.get()));
        }
    }

    public long getTerm(long index) {
        if (index > lastToken.get()) {
            return -1;
        }
        long segment = getSegmentFor(index);
        ByteBufferEntrySource entrySource = readBuffers.get(segment);
        if (entrySource != null) {
            Integer position = positionsPerSegmentMap.get(segment).get(index);
            if (position == null) {
                return -1;
            }
            return entrySource.readTerm(position);
        }
        if (next != null) {
            return next.getTerm(index);
        }
        return -1;
    }

    private void initLatestSegment(long lastInitialized, long nextToken, File storageDir, boolean clear) {
        long first = getFirstFile(lastInitialized, storageDir);
        WritableEntrySource buffer = getOrOpenDatafile(first);
        indexManager.remove(first);
        FileUtils.delete(storageProperties.indexFile(context, first));
        long sequence = first;
        try (SegmentEntryIterator iterator = buffer.createLogEntryIterator(first, EntrySource.START_POSITION)) {
            Map<Long, Integer> indexPositions = new ConcurrentHashMap<>();
            positionsPerSegmentMap.put(first, indexPositions);
            while (sequence < nextToken && iterator.hasNext()) {
                int position = iterator.position();
                Entry event = iterator.next();
                indexPositions.put(event.getIndex(), position);
                sequence++;
            }

            lastToken.set(sequence - 1);
            buffer.position(iterator.position());
        }

        buffer.putInt(buffer.position(), 0);

        WritePosition writePosition = new WritePosition(sequence, buffer.position(), buffer, first);
        writePositionRef.set(writePosition);

        if (clear) {
            buffer.clearFromPosition();
        }

        if (next != null) {
            next.initSegments(first);
        }
        synchronizer.init(writePosition);
    }

    private long getFirstFile(long lastInitialized, File events) {
        String[] entrylogFiles = FileUtils.getFilesWithSuffix(events, storageProperties.getLogSuffix());

        return Arrays.stream(entrylogFiles)
                .map(this::getSegment)
                .filter(segment -> segment < lastInitialized)
                .max(Long::compareTo)
                .orElse(lastToken.get() + 1);
    }

    private PreparedTransaction prepareTransaction(byte[] bytes) {
        byte[] transformed = logEntryTransformer.transform(bytes);
        return new PreparedTransaction(claim(transformed.length), transformed);
    }

    private CompletableFuture<Long> store(long term, int type, PreparedTransaction preparedTransaction) {
        logger.trace("{}: Storing in {} term log entry of {} type", context, term, type);
        CompletableFuture<Long> completableFuture = new CompletableFuture<>();
        try {
            WritePosition writePosition = preparedTransaction.getClaim();
            synchronizer.register(writePosition, new StorageCallback() {
                private final AtomicBoolean execute = new AtomicBoolean(true);

                @Override
                public boolean onCompleted(long firstToken) {
                    if (execute.getAndSet(false)) {
                        positionsPerSegmentMap.get(writePosition.segment).put(writePosition.sequence, writePosition.position);
                        completableFuture.complete(firstToken);
                        lastToken.set(firstToken);
                        return true;
                    }
                    return false;
                }

                @Override
                public void onError(Throwable cause) {
                    completableFuture.completeExceptionally(cause);
                }
            });
            write(writePosition, term, type, preparedTransaction.getTransformedData());
            synchronizer.notifyWritePositions();
        } catch (RuntimeException cause) {
            completableFuture.completeExceptionally(cause);
        }

        return completableFuture;
    }

    @Override
    public void handover(Long segment, Runnable callback) {
        callback.run();
    }

    @Override
    public void cleanup(int delay) {
        synchronizer.shutdown(false);
        readBuffers.forEach((s, source) -> source.clean(delay));
        if (next != null) next.cleanup(delay);
    }

    @Override
    protected SortedSet<Long> getSegments() {
        return positionsPerSegmentMap.descendingKeySet();
    }

    @Override
    protected Optional<EntrySource> getEventSource(long segment) {
        if (readBuffers.containsKey(segment)) {
            return Optional.of(readBuffers.get(segment).duplicate());
        }
        return Optional.empty();
    }


    public long getLastToken() {
        return lastToken.get();
    }


    @Override
    public Stream<String> getBackupFilenames(long lastSegmentBackedUp) {
        return next != null ? next.getBackupFilenames(lastSegmentBackedUp) : Stream.empty();
    }

    @Override
    public void rollback(long index) {
        logger.info("{}: Rolling back to index {}", context, index);
        if (index >= getLastToken()) {
            return;
        }
        synchronizer.shutdown(false);

        for (long segment : getSegments()) {
            if (segment > index) {
                removeSegment(segment);
            }
        }

        if (positionsPerSegmentMap.isEmpty() && next != null) {
            next.rollback(index);
        }

        initLatestSegment(Long.MAX_VALUE, index + 1, new File(storageProperties.getStorage(context)), true);
    }

    @Override
    protected void recreateIndex(long segment) {
        // No implementation as for primary segment store there are no index files, index is kept in memory
    }

    protected void removeSegment(long segment) {
        logger.info("{}: Removing {} segment", context, segment);
        positionsPerSegmentMap.remove(segment);
        ByteBufferEntrySource eventSource = readBuffers.remove(segment);
        if (eventSource != null) eventSource.clean(0);
        FileUtils.delete(storageProperties.logFile(context, segment));
    }

    private void completeSegment(Long segment) {
        try {
            logger.debug("{}: Completing segment {}", context, segment);
            indexManager.createIndex(segment, positionsPerSegmentMap.get(segment), false);
        } catch (RuntimeException re) {
            logger.warn("{}: Failed to create index", context, re);
        }
        if (next != null) {
            next.handover(segment, () -> {
                positionsPerSegmentMap.remove(segment);
                ByteBufferEntrySource source = readBuffers.remove(segment);
                logger.debug("{}: Handed over {}, remaining segments: {}",
                             context,
                             segment,
                             positionsPerSegmentMap.keySet());
                source.clean(storageProperties.getPrimaryCleanupDelay());
            });
        }
    }

    private void write(WritePosition writePosition, long term, int type, byte[] bytes) {
        ByteBuffer writeBuffer = writePosition.buffer.duplicate().getBuffer();
        writeBuffer.position(writePosition.position);
        writeBuffer.putInt(0);
        writeBuffer.put(VERSION);
        writeBuffer.putLong(term);
        writeBuffer.putInt(type);
        Checksum checksum = new Checksum();
        int eventsPosition = writeBuffer.position();
        writeBuffer.put(bytes);
        writeBuffer.putInt(checksum.update(writeBuffer, eventsPosition, writeBuffer.position() - eventsPosition).get());
        writeBuffer.position(writePosition.position);
        writeBuffer.putInt(bytes.length);
    }

    private WritePosition claim(int eventBlockSize) {
        int totalSize = HEADER_BYTES + eventBlockSize + TX_CHECKSUM_BYTES;
        if (totalSize > storageProperties.getSegmentSize() - 9) {
            throw new LogException(ErrorCode.PAYLOAD_TOO_LARGE,
                                   context + ": Size of transaction too large, max size = " + (
                                           storageProperties.getSegmentSize() - 9));
        }
        WritePosition writePosition;
        do {
            writePosition = writePositionRef.getAndAccumulate(
                    new WritePosition(1, totalSize),
                    (prev, x) -> prev.incrementedWith(x.sequence, x.position));

            if (writePosition.isOverflow(totalSize)) {
                // only one thread can be here
                logger.debug("{}: Creating new segment {}", context, writePosition.sequence);

                writePosition.buffer.putInt(writePosition.position, -1);

                WritableEntrySource buffer = getOrOpenDatafile(writePosition.sequence);
                writePositionRef.set(writePosition.reset(buffer));
            }
        } while (!writePosition.isWritable(totalSize));

        return writePosition;
    }

    private WritableEntrySource getOrOpenDatafile(long segment) {
        File file = storageProperties.logFile(context, segment);
        long size = storageProperties.getSegmentSize();
        boolean exists = file.exists();
        if (exists) {
            size = file.length();
            logger.debug("{}: File for segment {} already exists with size {}. Reopening.",
                         context,
                         segment,
                         size);
        } else {
            logger.info("{}: File for segment {} does not exist. Creating new file with size of {}.",
                        context,
                        segment,
                        size);
        }
        try (FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel()) {
            positionsPerSegmentMap.computeIfAbsent(segment, k -> new ConcurrentHashMap<>());
            MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, size);
            int bufferLimit = buffer.limit();
            logger.debug("{}: Opened buffer for segment file {} with limit {}.",
                         context,
                         segment,
                         bufferLimit);
            checkBuffer(segment, size, bufferLimit);
            if (!exists) {
                buffer.put(VERSION);
                buffer.putInt(storageProperties.getFlags());
            } else {
                buffer.position(5);
            }
            WritableEntrySource writableEventSource = new WritableEntrySource(buffer,
                                                                              logEntryTransformer,
                                                                              storageProperties
                                                                                      .isForceCleanMmapIndex());
            readBuffers.put(segment, writableEventSource);
            return writableEventSource;
        } catch (IOException ioException) {
            throw new LogException(ErrorCode.DATAFILE_READ_ERROR,
                                   context + ": Failed to open segment: " + segment,
                                   ioException);
        }
    }

    private void checkBuffer(long segment, long size, int bufferLimit) {
        if (bufferLimit != size) {
            logger.warn(
                    "{}: Buffer limit of {} and segment size of {} do not match. Did you change segment size in storage properties?",
                    context,
                    bufferLimit,
                    size);
        }
        if (bufferLimit == 0) {
            String message =
                    context + ": Segment file " + segment + " has 0 buffer limit and size of " + size
                            + ". It looks like it's corrupted. Aborting.";
            logger.error(message);
            throw new LogException(ErrorCode.DATAFILE_READ_ERROR, message);
        }
    }

    public void setNext(SegmentBasedLogEntryStore secondaryEventStore) {
        next = secondaryEventStore;
    }

    public void clear(long lastIndex) {
        logger.info("{}: Clearing log entries, setting last index to {}", context, lastIndex);
        if (next != null) {
            next.getSegments().forEach(segment -> next.removeSegment(segment));
        }
        getSegments().forEach(this::removeSegment);
        cleanup(0);
        positionsPerSegmentMap.clear();
        lastToken.set(lastIndex);
    }


    public void delete() {
        logger.info("{}: Deleting context.", context);
        clear(0);
        File storageDir = new File(storageProperties.getStorage(context));
        FileUtils.delete(storageDir);
        synchronizer.shutdown(true);
    }

    public void clearOlderThan(long time, TimeUnit timeUnit, LongSupplier lastAppliedIndexSupplier) {
        if (next != null) {
            next.clearOlderThan(time, timeUnit, lastAppliedIndexSupplier);
        }
    }

    @NotNull
    private Long getSegment(String segmentName) {
        return Long.valueOf(segmentName.substring(0, segmentName.indexOf('.')));
    }

    @Override
    public boolean isClosed() {
        return synchronizer.isShutdown();
    }

    @Override
    public void close(boolean deleteData) {
        synchronizer.shutdown(true);
        readBuffers.forEach((s, source) -> {
            source.clean(0);
            if (deleteData) removeSegment(s);
        });

        if (next != null) next.close(deleteData);

        if (deleteData) {
            delete();
        }

        indexManager.cleanup();
    }
}
