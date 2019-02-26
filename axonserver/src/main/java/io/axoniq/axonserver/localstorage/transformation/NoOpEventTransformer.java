package io.axoniq.axonserver.localstorage.transformation;

/**
 * @author Marc Gathier
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
