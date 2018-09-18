package io.axoniq.axonserver.localstorage.transformation;

import io.axoniq.axonserver.grpc.event.Event;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Author: marc
 */
public abstract class EventTransformer {


    public Event readEvent(byte[] eventBytes) {
        return transform(eventBytes);
    }

    protected abstract Event transform(byte[] eventBytes);

    private byte[] getEventBytes(ByteBuffer buffer, int position) {
        buffer.position(position);
        return getEventBytes(buffer);

    }
    private byte[] getEventBytes(ByteBuffer buffer) {
        int size = buffer.getInt();
        byte[] bytes = new byte[size];
        buffer.get(bytes);
        return bytes;
    }

    public List<ProcessedEvent> transform(List<Event> origEventList) {
        return origEventList.stream().map(this::transform).collect(Collectors.toList());
    }

    protected abstract ProcessedEvent transform(Event e);
}
