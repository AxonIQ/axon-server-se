package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;

/**
 * Author: marc
 */
public interface ReplicationConnection {

    int sendNextEntries(LeaderState.PeerInfo peer);

    void send(AppendEntriesRequest heartbeat);
}
