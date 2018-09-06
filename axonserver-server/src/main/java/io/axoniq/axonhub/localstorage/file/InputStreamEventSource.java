package io.axoniq.axonhub.localstorage.file;

import io.axoniq.axondb.Event;
import io.axoniq.axonhub.exception.ErrorCode;
import io.axoniq.axonhub.exception.MessagingPlatformException;
import io.axoniq.axonhub.localstorage.transformation.EventTransformer;
import io.axoniq.axonhub.localstorage.transformation.EventTransformerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Author: marc
 */
public class InputStreamEventSource implements EventSource {
    private final static Logger logger = LoggerFactory.getLogger(InputStreamEventSource.class);
    private final PositionKeepingDataInputStream dataInputStream;
    private final EventTransformer eventTransformer;

    public InputStreamEventSource(File dataFile,
                                  EventTransformerFactory eventTransformerFactory,
                                  StorageProperties storageProperties) {
        try {
            logger.debug("Open file {}", dataFile);
            dataInputStream = new PositionKeepingDataInputStream(new FileInputStream(dataFile));
            byte version = dataInputStream.readByte();
            int modifiers = dataInputStream.readInt();
            eventTransformer = eventTransformerFactory.get(version, modifiers, storageProperties);
        } catch (IOException e) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR, e.getMessage(), e);
        }
    }

    @Override
    public Event readEvent(int position)  {
        try {
            dataInputStream.position(position);
            return readEvent();
        } catch (IOException ioException) {
            throw new MessagingPlatformException(ErrorCode.DATAFILE_READ_ERROR, ioException.getMessage(), ioException);
        }
    }

    public Event readEvent() throws IOException {
        byte[] bytes = dataInputStream.readEvent();
        return eventTransformer.readEvent(bytes);
    }

    @Override
    public TransactionIterator createTransactionIterator(long segment, long token, boolean validating) {
        return new InputStreamTransactionIterator(this, segment, token, validating);
    }

    @Override
    public EventIterator createEventIterator(long segment, long startToken) {
        return new InputStreamEventIterator(this, segment, startToken);
    }

    public PositionKeepingDataInputStream getStream() {
        return dataInputStream;
    }

    @Override
    public void close()  {
        try {
            dataInputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
