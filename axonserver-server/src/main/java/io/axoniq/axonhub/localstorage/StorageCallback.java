package io.axoniq.axonhub.localstorage;

/**
 * Author: marc
 */
public interface StorageCallback {
    boolean onCompleted(long firstToken);

    void onError(Throwable cause);

}
