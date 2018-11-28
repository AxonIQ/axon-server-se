package io.axoniq.axonserver.cluster.replication.file;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.cluster.TermIndex;
import io.axoniq.axonserver.cluster.exception.ErrorCode;
import io.axoniq.axonserver.cluster.exception.LogException;
import io.axoniq.axonserver.cluster.replication.EntryIterator;
import io.axoniq.axonserver.cluster.replication.LogEntryStore;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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
    private final PrimaryEventStore primaryEventStore;

    public FileSegmentLogEntryStore(PrimaryEventStore primaryEventStore) {
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

    private void registerEntry(WritePosition writePosition, Runnable onComplete) {
    }

    private void write(WritePosition writePosition, long currentTerm, int dataType, byte[] data) {
    }

    private WritePosition claim(int serializedSize) {
        return null;
    }

    @Override
    public void appendEntry(List<Entry> entries) throws IOException {
        entries.forEach(e -> {
            Entry existingEntry = getEntry(e.getIndex());
            boolean skip = false;
            if( existingEntry != null ) {
                if( existingEntry.getTerm() != e.getTerm() ) {
                    deleteFrom(e.getIndex());
                }
            } else {
                skip = true;
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
        primaryEventStore.rollback(index-1);
    }

    @Override
    public boolean contains(long logIndex, long logTerm) {
        Entry existingEntry = getEntry(logIndex);
        return existingEntry != null && existingEntry.getTerm() == logTerm;
    }

    @Override
    public int applyEntries(Consumer<Entry> consumer) {
        return 0;
    }

    @Override
    public void markCommitted(long committedIndex) {

    }

    @Override
    public long commitIndex() {
        return 0;
    }

    @Override
    public long lastAppliedIndex() {
        return 0;
    }

    @Override
    public Entry getEntry(long index) {
        return primaryEventStore.getEntry(index);
    }

    @Override
    public long lastLogTerm() {
        return 0;
    }

    @Override
    public long lastLogIndex() {
        return 0;
    }


    @Override
    public TermIndex lastLog() {
        return null;
    }

    @Override
    public EntryIterator createIterator(long index) {
        return null;
    }

    @Override
    public void registerCommitListener(Thread currentThread) {

    }

    @Override
    public void clear(long logIndex, long logTerm) {

    }
}
