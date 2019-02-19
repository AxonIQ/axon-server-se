package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.election.ElectionStore;
import io.axoniq.axonserver.cluster.replication.LogEntryStore;
import io.axoniq.axonserver.grpc.cluster.Node;

public interface RaftGroup {

    LogEntryStore localLogEntryStore();

    ElectionStore localElectionStore();

    RaftConfiguration raftConfiguration();

    LogEntryProcessor logEntryProcessor();

    /**
     * Returns the last safe persisted sequence in the store for type Event.
     *
     * @return the last persisted Event's sequence
     */
    default long lastAppliedEventSequence() {
        return -1L;
    }

    /**
     * Returns the last safe persisted sequence in the store for type Snapshot.
     *
     * @return  the last persisted Snapshot's sequence
     */
    default long lastAppliedSnapshotSequence() {
        return -1L;
    }

    RaftPeer peer(String nodeId);

    RaftPeer peer(Node node);

    RaftNode localNode();

    default void connect() {
        localNode().start();
    }

    default void delete() {
        localLogEntryStore().delete();
        raftConfiguration().delete();
        localElectionStore().delete();
    }
}
