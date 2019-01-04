package io.axoniq.axonserver.localstorage.transformation;

/**
 * Author: marc
 */
public class NoOpEventTransformer extends EventTransformer {

    public static final NoOpEventTransformer INSTANCE = new NoOpEventTransformer();

    private NoOpEventTransformer() {

    }

    @Override
    protected byte[] fromStorage(byte[] eventBytes) {
        return eventBytes;
    }

    @Override
    public byte[] toStorage(byte[] event) {
        return event;
    }
}
