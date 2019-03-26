package io.axoniq.axonserver.localstorage.transformation;

/**
 * @author Marc Gathier
 */
public class NoOpEventTransformer implements EventTransformer {

    public static final NoOpEventTransformer INSTANCE = new NoOpEventTransformer();

    private NoOpEventTransformer() {
    }

    @Override
    public byte[] fromStorage(byte[] eventBytes) {
        return eventBytes;
    }

    @Override
    public byte[] toStorage(byte[] event) {
        return event;
    }
}
