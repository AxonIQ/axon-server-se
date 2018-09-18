package io.axoniq.axonserver.localstorage;

/**
 * Author: marc
 */
public interface StorageCallback {
    boolean onCompleted(long firstToken);

    void onError(Throwable cause);

}
