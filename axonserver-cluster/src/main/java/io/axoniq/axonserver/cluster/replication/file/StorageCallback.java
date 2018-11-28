package io.axoniq.axonserver.cluster.replication.file;

/**
 * Author: marc
 */
public interface StorageCallback  {
    boolean onCompleted(long firstToken);
    void onError(Throwable cause);
}
