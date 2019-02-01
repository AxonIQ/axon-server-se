package io.axoniq.axonserver.localstorage;

/**
 * @author Marc Gathier
 */
public interface StorageCallback {
    boolean onCompleted(long firstToken);

    void onError(Throwable cause);

}
