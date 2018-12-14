package io.axoniq.axonserver.localstorage.file;

import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.internal.TransactionWithToken;
import io.axoniq.axonserver.localstorage.TransactionInformation;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Author: marc
 */
public class InputStreamTransactionIterator implements TransactionIterator {

    private final InputStreamEventSource eventSource;
    private final PositionKeepingDataInputStream reader;
    private long currentSequenceNumber;
    private final boolean validating;
    private TransactionWithToken next;
    private TransactionInformation currentTransaction;

    public InputStreamTransactionIterator(InputStreamEventSource eventSource, long segment, long start, boolean validating) {
        this.eventSource = eventSource;
        this.reader = eventSource.getStream();
        this.currentSequenceNumber = segment;
        this.validating = validating;
        try {
            forwardTo(start);
        } catch (IOException e) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR, e.getMessage(), e);
        }
        readTransaction();
    }

    private void forwardTo(long firstSequence) throws IOException {
        while (firstSequence > currentSequenceNumber) {

            int size = reader.readInt();
            if (size == -1 || size == 0) {
                return;
            }
            processVersion(reader);
            short nrOfMessages = reader.readShort();

            if (firstSequence >= currentSequenceNumber + nrOfMessages) {
                reader.skipBytes(size + 4);
                currentSequenceNumber += nrOfMessages;
            } else {
                throw new MessagingPlatformException(ErrorCode.INVALID_TRANSACTION_TOKEN,
                                                     "FirstSequence in middle of transaction, firstSequence=" + firstSequence + ", current="
                                                             + currentSequenceNumber + ", nrOfMessages=" + nrOfMessages);
            }
        }
    }

    private void processVersion(PositionKeepingDataInputStream reader) throws IOException {
        byte version = reader.readByte();
        currentTransaction = new TransactionInformation(version, reader);
    }

    private void addEvent(TransactionWithToken.Builder transactionWithTokenBuilder) {
        try {
            transactionWithTokenBuilder.addEvents(eventSource.readEvent());
            currentSequenceNumber++;
        } catch (IOException | RuntimeException io) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR, "Failed to read event: " + currentSequenceNumber, io);
        }
    }

    private boolean readTransaction() {
        try {
            int size = reader.readInt();
            if (size == -1 || size == 0) {
                return false;
            }
            processVersion(reader);
            TransactionWithToken.Builder transactionWithTokenBuilder = TransactionWithToken.newBuilder()
                                                                                           .setToken(currentSequenceNumber)
                                                                                           .setIndex(currentTransaction.getIndex());

            short nrOfMessages = reader.readShort();
            for (int idx = 0; idx < nrOfMessages; idx++) {
                addEvent(transactionWithTokenBuilder);
            }
            next = transactionWithTokenBuilder.build();
            int chk = reader.readInt(); // checksum
//            if (validating) {
//                Checksum checksum = new Checksum();
//                checksum.update(reader, position, size);
//                if( chk != checksum.get()) {
//                    throw new RuntimeException("Invalid checksum at " + currentSequenceNumber);
//                }
//            }
            return true;
        } catch (IOException | RuntimeException io) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR, "Failed to read event: " + currentSequenceNumber, io);
        }
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public TransactionWithToken next() {
        if (next == null) {
            throw new NoSuchElementException();
        }
        TransactionWithToken rv = next;
        if (!readTransaction()) {
            next = null;
            close();
        }
        return rv;
    }

    @Override
    public void close() {
        next = null;
        eventSource.close();
    }

    @Override
    public TransactionInformation currentTransaction() {
        return currentTransaction;
    }
}
