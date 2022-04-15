package io.axoniq.axonserver.localstorage;

import org.springframework.data.util.CloseableIterator;

public interface EventIteratorProvider {

    CloseableIterator<SerializedEventWithToken> iteratorFrom(long token);
}
