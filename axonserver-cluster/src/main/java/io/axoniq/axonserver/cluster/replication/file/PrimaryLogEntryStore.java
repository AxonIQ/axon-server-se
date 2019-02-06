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
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Author: marc
 */
public class PrimaryLogEntryStore extends SegmentBasedLogEntryStore {
    private static final Logger logger = LoggerFactory.getLogger(PrimaryLogEntryStore.class);
    private static final int HEADER_BYTES = 4 + 1 + 8 + 4;
    private static final int TX_CHECKSUM_BYTES = 4;

    private final LogEntryTransformerFactory eventTransformerFactory;
    private final Synchronizer synchronizer;
    private final AtomicReference<WritePosition> writePositionRef = new AtomicReference<>();
    private final AtomicLong lastToken = new AtomicLong(0);
    private final ConcurrentNavigableMap<Long, Map<Long, Integer>> positionsPerSegmentMap = new ConcurrentSkipListMap<>();
    private final Map<Long, ByteBufferEntrySource> readBuffers = new ConcurrentHashMap<>();
    private LogEntryTransformer eventTransformer;

    public PrimaryLogEntryStore(String context, IndexManager indexCreator, LogEntryTransformerFactory eventTransformerFactory, StorageProperties storageProperties) {
        super(context, indexCreator, storageProperties);
        this.eventTransformerFactory = eventTransformerFactory;
        synchronizer = new Synchronizer(context, storageProperties, this::completeSegment);
    }

    @Override
    public void initSegments(long lastInitialized) {
        File storageDir  = new File(storageProperties.getStorage(getType()));
        FileUtils.checkCreateDirectory(storageDir);
        eventTransformer = eventTransformerFactory.get(VERSION, storageProperties.getFlags(), storageProperties);
        initLatestSegment(lastInitialized, Long.MAX_VALUE, storageDir, false);
    }

    public CompletableFuture<Long> write(long term, int type, byte[] bytes) {
        if( bytes.length == 0) throw new LogException(ErrorCode.VALIDATION_FAILED, "Cannot store empty log entry");
        PreparedTransaction preparedTransaction = prepareTransaction(bytes);
        return store(term, type, preparedTransaction);
    }

    public SegmentEntryIterator getIterator(long nextIndex) {
        if( nextIndex > lastToken.get()) return null;
        logger.trace("Create iterator at: " + nextIndex);
        return super.getIterator(nextIndex);
    }

    public EntryIterator getEntryIterator(long nextIndex) {
        return new MultiSegmentIterator(this, nextIndex);
    }

    public Entry getEntry(long index) {
        if( index > lastToken.get()) return null;
        long segment = getSegmentFor(index);
        ByteBufferEntrySource entrySource = readBuffers.get(segment);
        if( entrySource != null) {
            Integer position = positionsPerSegmentMap.get(segment).get(index);
            if( position == null) {
                return null;
            }
            return entrySource.duplicate().readLogEntry(position, index);
        }
        if( next != null) {
            return next.getEntry(index);
        }
        return null;
    }

    @Override
    protected int getPosition(long segment, long nextIndex) {
        return positionsPerSegmentMap.get(segment).get(nextIndex);
    }

    private void initLatestSegment(long lastInitialized, long nextToken, File storageDir, boolean clear) {
        long first = getFirstFile(lastInitialized, storageDir);
        WritableEntrySource buffer = getOrOpenDatafile(first);
        indexManager.remove(first);
        FileUtils.delete(storageProperties.indexFile(getType(), first));
        long sequence = first;
        try (SegmentEntryIterator iterator = buffer.createLogEntryIterator(first, first, 5, false)) {
            Map<Long, Integer> indexPositions = new ConcurrentHashMap<>();
            positionsPerSegmentMap.put(first, indexPositions);
            while (sequence < nextToken && iterator.hasNext()) {
                Entry event = iterator.next();
                indexPositions.put(event.getIndex(), iterator.startPosition());
                sequence++;
            }
            lastToken.set(sequence - 1);
            buffer.position(iterator.startPosition());
        }

        buffer.putInt(buffer.position(), 0);

        WritePosition writePosition = new WritePosition(sequence, buffer.position(), buffer, first);
        writePositionRef.set(writePosition);
        synchronizer.init(writePosition);

        if( clear)
            buffer.clearFromPosition();

        if( next != null) {
            next.initSegments(first);
        }
    }

    private long getFirstFile(long lastInitialized, File events) {
        String[] eventFiles = FileUtils.getFilesWithSuffix(events, storageProperties.getLogSuffix());

        return Arrays.stream(eventFiles)
                     .map(this::getSegment)
                     .filter(segment -> segment < lastInitialized)
                     .max(Long::compareTo)
                     .orElse(1L);
    }

    private PreparedTransaction prepareTransaction(byte[] bytes) {
        byte[] transformed = eventTransformer.transform(bytes);
        return new PreparedTransaction(claim(transformed.length),  transformed);
    }

    private CompletableFuture<Long> store(long term, int type, PreparedTransaction preparedTransaction) {
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
        synchronizer.shutdown(true);
        readBuffers.forEach((s, source) -> source.clean(delay));
        if( next != null) next.cleanup(delay);
    }

    @Override
    protected SortedSet<Long> getSegments() {
        return positionsPerSegmentMap.descendingKeySet();
    }

    @Override
    protected Optional<EntrySource> getEventSource(long segment) {
        if( readBuffers.containsKey(segment) ) {
            return Optional.of(readBuffers.get(segment).duplicate());
        }
        return Optional.empty();
    }


    public long getLastToken() {
        return lastToken.get();
    }


    @Override
    public Stream<String> getBackupFilenames(long lastSegmentBackedUp) {
        return next!= null ? next.getBackupFilenames(lastSegmentBackedUp): Stream.empty();
    }

    @Override
    public void rollback( long token) {
        if( token >= getLastToken()) {
            return;
        }
        synchronizer.shutdown(false);

        for( long segment: getSegments()) {
            if( segment > token) {
                removeSegment(segment);
            }
        }

        if( positionsPerSegmentMap.isEmpty() && next != null) {
            next.rollback(token);
        }

        initLatestSegment(Long.MAX_VALUE, token+1, new File(storageProperties.getStorage(getType())), true);
    }

    @Override
    protected void recreateIndex(long segment) {
        // No implementation as for primary segment store there are no index files, index is kept in memory
    }

    private void removeSegment(long segment) {
        positionsPerSegmentMap.remove(segment);
        ByteBufferEntrySource eventSource = readBuffers.remove(segment);
        if( eventSource != null) eventSource.clean(0);
        FileUtils.delete(storageProperties.logFile(getType(), segment));
    }

    private void completeSegment(WritePosition writePosition) {
        try {
            indexManager.createIndex(writePosition.segment, positionsPerSegmentMap.get(writePosition.segment), false);
        } catch( RuntimeException re) {
            logger.warn("Failed to create index", re);
        }
        if( next != null) {
            next.handover(writePosition.segment, () -> {
                positionsPerSegmentMap.remove(writePosition.segment);
                ByteBufferEntrySource source = readBuffers.remove(writePosition.segment);
                logger.debug("Handed over {}, remaining segments: {}", writePosition.segment, positionsPerSegmentMap.keySet());
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

    private WritePosition claim(int eventBlockSize)  {
        int totalSize = HEADER_BYTES + eventBlockSize + TX_CHECKSUM_BYTES;
        if( totalSize > storageProperties.getSegmentSize()-9)
            throw new LogException(ErrorCode.PAYLOAD_TOO_LARGE, "Size of transaction too large, max size = " + (storageProperties.getSegmentSize() - 9));
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

    private WritableEntrySource getOrOpenDatafile(long segment)  {
        File file= storageProperties.logFile(getType(), segment);
        long size = storageProperties.getSegmentSize();
        boolean exists = file.exists();
        if( exists) {
            size = file.length();
        }
        logger.debug("Open file: {}", file);
        try(FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel()) {
            positionsPerSegmentMap.computeIfAbsent(segment, k -> new ConcurrentHashMap<>());
            MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, size);
            if(!exists) {
                buffer.put(VERSION);
                buffer.putInt(storageProperties.getFlags());
            } else {
                buffer.position(5);
            }
            WritableEntrySource writableEventSource = new WritableEntrySource(buffer, eventTransformer);
            readBuffers.put(segment, writableEventSource);
            return writableEventSource;
        } catch (IOException ioException) {
            throw new LogException(ErrorCode.DATAFILE_READ_ERROR, "Failed to open segment: " + segment, ioException);
        }
    }

    public void setNext(SegmentBasedLogEntryStore secondaryEventStore) {
        next = secondaryEventStore;
    }

    public void clear() {
        cleanup(0);
        File storageDir  = new File(storageProperties.getStorage(getType()));
        String[] logFiles = FileUtils.getFilesWithSuffix(storageDir, storageProperties.getLogSuffix());
        String[] indexFiles = FileUtils.getFilesWithSuffix(storageDir, storageProperties.getIndexSuffix());
        Stream.of(logFiles, indexFiles)
              .flatMap(Stream::of)
              .map(filename -> storageDir.getAbsolutePath() + File.separator + filename)
              .map(File::new)
              .forEach(FileUtils::delete);
        lastToken.set(0);
    }

    public void delete() {
        clear();
        File storageDir  = new File(storageProperties.getStorage(getType()));
        storageDir.delete();
    }

    public void clearOlderThan(long time, TimeUnit timeUnit, Supplier<Long> lastAppliedIndexSupplier) {
        File storageDir  = new File(storageProperties.getStorage(getType()));
        String[] logFiles = FileUtils.getFilesWithSuffix(storageDir, storageProperties.getLogSuffix());
        String[] indexFiles = FileUtils.getFilesWithSuffix(storageDir, storageProperties.getIndexSuffix());
        long filter = System.currentTimeMillis() - timeUnit.toMillis(time);
        Stream.of(logFiles, indexFiles)
              .flatMap(Stream::of)
              .filter(name -> !readBuffers.containsKey(getSegment(name))) // filter out opened files
              .filter(name -> getSegment(name) < getFirstFile(lastAppliedIndexSupplier.get() - 1, storageDir)) // filter out non-applied files
              .map(filename -> storageDir.getAbsolutePath() + File.separator + filename)
              .map(File::new)
              .filter(f -> f.lastModified() <= filter) // filter out files older than <time>
              .forEach(FileUtils::delete);
    }

    @NotNull
    private Long getSegment(String segmentName) {
        return Long.valueOf(segmentName.substring(0, segmentName.indexOf('.')));
    }
}
