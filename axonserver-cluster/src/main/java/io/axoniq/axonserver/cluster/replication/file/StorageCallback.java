package io.axoniq.axonserver.cluster.replication.file;

/**
 * @author Marc Gathier
 */
public interface StorageCallback  {
    boolean onCompleted(long firstToken);
    void onError(Throwable cause);
}
