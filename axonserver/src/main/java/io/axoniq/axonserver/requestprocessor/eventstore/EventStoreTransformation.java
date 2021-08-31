package io.axoniq.axonserver.requestprocessor.eventstore;

import io.axoniq.axonserver.localstorage.SerializedEventWithToken;
import org.springframework.data.util.CloseableIterator;

import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Marc Gathier
 * @since 4.6.0
 */
public class EventStoreTransformation {

    private String id;
    private String name;
    private long previousToken = -1;
    private boolean applying;
    private AtomicReference<CloseableIterator<SerializedEventWithToken>> iterator = new AtomicReference<>();

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getPreviousToken() {
        return previousToken;
    }

    public void setPreviousToken(long previousToken) {
        this.previousToken = previousToken;
    }

    public boolean isApplying() {
        return applying;
    }

    public void setApplying(boolean applying) {
        this.applying = applying;
    }

    public CloseableIterator<SerializedEventWithToken> getIterator() {
        return iterator.get();
    }

    public void setIterator(
            CloseableIterator<SerializedEventWithToken> iterator) {
        this.iterator.set(iterator);
    }

    public void closeIterator() {
        CloseableIterator<SerializedEventWithToken> activeIterator = iterator.getAndSet(null);
        if( activeIterator != null) {
            activeIterator.close();
        }
    }
}
