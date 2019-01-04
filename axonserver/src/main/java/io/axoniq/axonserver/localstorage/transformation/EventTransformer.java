package io.axoniq.axonserver.localstorage.transformation;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Author: marc
 */
public abstract class EventTransformer {


    public byte[] readEvent(byte[] eventBytes) {
        return fromStorage(eventBytes);
    }

    protected abstract byte[] fromStorage(byte[] eventBytes);

    public List<byte[]> toStorage(List<byte[]> origEventList) {
        return origEventList.stream().map(this::toStorage).collect(Collectors.toList());
    }

    public abstract byte[] toStorage(byte[] e);
}
