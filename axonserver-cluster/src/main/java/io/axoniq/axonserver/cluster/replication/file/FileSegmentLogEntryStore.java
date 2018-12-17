package io.axoniq.axonserver.cluster.replication.file;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.cluster.Registration;
import io.axoniq.axonserver.cluster.TermIndex;
import io.axoniq.axonserver.cluster.exception.ErrorCode;
import io.axoniq.axonserver.cluster.exception.LogException;
import io.axoniq.axonserver.cluster.replication.EntryIterator;
import io.axoniq.axonserver.cluster.replication.InMemoryEntryIterator;
import io.axoniq.axonserver.cluster.replication.LogEntryStore;
import io.axoniq.axonserver.grpc.cluster.Config;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

/**
 * File format is:
 *  #Version#Flags#LogEntries#EOF
 *  Version: byte indicating the file version
 *  Flags: int, providing additional flags, e.g. compression/encryption
 *  LogEntries: list of entries each in format:
 *      #DataSize#Version#Term#Data#CRC
 *      DataSize: int, the length of the Data block
 *      Version: byte, version indicator for entry
 *      Term: long, the term in which the entry was created
 *      DataType: int, number of type of data in proto definition of Entry (3 for serializedObject, 4 for newConfiguration)
 *      Data: bytes: Protobuf bytes
 *  EOF: int, -1 when file is complete
 *
 * Author: marc
 */
public class FileSegmentLogEntryStore implements LogEntryStore {
    private final Logger logger = LoggerFactory.getLogger(FileSegmentLogEntryStore.class);
    private final List<Consumer<Entry>> appendListeners = new CopyOnWriteArrayList<>();
    private final List<Consumer<Entry>> rollbackListeners = new CopyOnWriteArrayList<>();
    private final String name;

    private final PrimaryLogEntryStore primaryEventStore;

    public FileSegmentLogEntryStore(String name, PrimaryLogEntryStore primaryEventStore) {
        this.name = name;
        this.primaryEventStore = primaryEventStore;
    }

    @Override
    public CompletableFuture<Entry> createEntry(long currentTerm, String entryType, byte[] entryData) {
        CompletableFuture<Entry> completableFuture = new CompletableFuture<>();
        try {
            SerializedObject serializedObject = SerializedObject.newBuilder()
                                                                .setData(ByteString.copyFrom(entryData))
                                                                .setType(entryType)
                                                                .build();
            CompletableFuture<Long> writeCompleted = primaryEventStore.write(currentTerm,
                                                                             Entry.DataCase.SERIALIZEDOBJECT.getNumber(),
                                                                             serializedObject.toByteArray());

            writeCompleted.whenComplete((index, throwable ) -> {
                if( throwable != null) {
                    completableFuture.completeExceptionally(throwable);
                } else {
                    completableFuture.complete(Entry.newBuilder()
                                                    .setTerm(currentTerm)
                                                    .setIndex(index)
                                                    .setSerializedObject(serializedObject)
                                                    .build());
                }
            });
        } catch( Exception ex) {
            completableFuture.completeExceptionally(ex);
        }

        return completableFuture;
    }

    @Override
    public CompletableFuture<Entry> createEntry(long currentTerm, Config config) {
        CompletableFuture<Entry> completableFuture = new CompletableFuture<>();
        try {
            CompletableFuture<Long> writeCompleted = primaryEventStore.write(currentTerm,
                                                                             Entry.DataCase.NEWCONFIGURATION.getNumber(),
                                                                             config.toByteArray());

            writeCompleted.whenComplete((index, throwable ) -> {
                if( throwable != null) {
                    completableFuture.completeExceptionally(throwable);
                } else {
                    logger.info("{}: written {}", name, index);
                    completableFuture.complete(Entry.newBuilder()
                                                    .setTerm(currentTerm)
                                                    .setIndex(index)
                                                    .setNewConfiguration(config)
                                                    .build());
                }
            });
        } catch( Exception ex) {
            completableFuture.completeExceptionally(ex);
        }

        return completableFuture;
    }

    @Override
    public void appendEntry(List<Entry> entries) throws IOException {
        entries.forEach(e -> {
            Entry existingEntry = getEntry(e.getIndex());
            boolean skip = false;
            if( existingEntry != null ) {
                if( existingEntry.getTerm() != e.getTerm() ) {
                    logger.warn("{}: Clear from {}", name, e.getIndex());
                    deleteFrom(e.getIndex());
                } else {
                    logger.warn("{}: Skip {}", name, e.getIndex());
                    skip = true;
                }
            }

            if( !skip) {
                CompletableFuture<Long> writeCompleted = null;
                switch (e.getDataCase()) {
                    case SERIALIZEDOBJECT:
                        writeCompleted = primaryEventStore.write(e.getTerm(), Entry.DataCase.SERIALIZEDOBJECT.getNumber(), e.getSerializedObject().toByteArray());
                        break;
                    case NEWCONFIGURATION:
                        writeCompleted = primaryEventStore.write(e.getTerm(), Entry.DataCase.NEWCONFIGURATION.getNumber(), e.getNewConfiguration().toByteArray());
                        break;
                    case DATA_NOT_SET:
                        break;
                }
                if( writeCompleted != null) {
                    try {
                        writeCompleted.get();
                        appendListeners.forEach(listener -> listener.accept(e));
                    } catch (InterruptedException e1) {
                        Thread.currentThread().interrupt();
                        throw new LogException(ErrorCode.INTERRUPTED, e1.getMessage());
                    } catch (ExecutionException e1) {
                        throw new LogException(ErrorCode.DATAFILE_WRITE_ERROR, e1.getMessage(), e1.getCause());
                    }
                }
            }
        });

    }

    private void deleteFrom(long index) {
        primaryEventStore.getEntryIterator(index).forEachRemaining(e -> rollbackListeners.forEach(l -> l.accept(e)));
        primaryEventStore.rollback(index-1);
    }

    @Override
    public boolean contains(long logIndex, long logTerm) {
        if( logIndex == 0) return true;
        Entry existingEntry = getEntry(logIndex);
        return existingEntry != null && existingEntry.getTerm() == logTerm;
    }
    @Override
    public Entry getEntry(long index) {
        if( index == 0) return null;
        return primaryEventStore.getEntry(index);
    }


    @Override
    public TermIndex lastLog() {
        Entry entry = getEntry(primaryEventStore.getLastToken());

        return entry == null ? new TermIndex(0, 0) : new TermIndex(entry.getTerm(), entry.getIndex());
    }

    @Override
    public long lastLogIndex() {
        return primaryEventStore.getLastToken();
    }

    @Override
    public Registration registerLogAppendListener(Consumer<Entry> listener) {
        appendListeners.add(listener);
        return () -> appendListeners.remove(listener);
    }

    @Override
    public Registration registerLogRollbackListener(Consumer<Entry> listener) {
        rollbackListeners.add(listener);
        return () -> rollbackListeners.remove(listener);
    }

    @Override
    public EntryIterator createIterator(long index) {
        if( index < primaryEventStore.getFirstToken()) throw new IllegalArgumentException("Read before start");
        return new InMemoryEntryIterator(this, index);
    }

    @Override
    public void clear(long logIndex, long logTerm) {
        Entry entry = getEntry(logIndex);
        if( entry == null && entry.getTerm() != logTerm) {
            // TODO: implement clear
            // primaryEventStore.clear();
        }

    }
}
