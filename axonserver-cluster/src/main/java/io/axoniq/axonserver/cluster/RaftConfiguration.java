package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.Config;
import io.axoniq.axonserver.grpc.cluster.Node;

import java.util.List;

public interface RaftConfiguration {

    String groupId();

    List<Node> groupMembers();

    void update(List<Node> newConf);

    default Config config() {
        return Config.newBuilder()
                     .addAllNodes(groupMembers())
                     .build();
    }

    default int minElectionTimeout() {
        return 150;
    }

    default int maxElectionTimeout() {
        return 300;
    }

    default int maxReplicationRound() {
        return 10;
    }

    default int heartbeatTimeout() {
        return 15;
    }

    default void delete() {}

    default int flowBuffer() {
        return 100;
    }

    default int maxEntriesPerBatch() {
        return 10;
    }

    /**
     * Define if the log compaction is enabled.
     * If true, the log entry files will be deleted when they become obsolete.
     *
     * @return {@code true} if the log compaction is enable, {@code false} otherwise
     */
    default boolean isLogCompactionEnabled() {
        return true;
    }

    /**
     * The number of hours closed logfiles should be retained before compacting the log.
     * @return number of hours
     */
    default int logRetentionHours() {
        return 1;
    }

    /**
     * Defines maximum message size for communication between nodes. Defaults to 4MB.
     * @return maximum message size
     */
    default int maxMessageSize() {
        return 4184304;
    }

    /**
     * Defines the maximum number of serializedObjects to be sent in a single InstallSnapshotRequest. Defaults to 10.
     * @return maximum number of serializedObjects sent in a single InstallSnapshotRequest
     */
    default int maxSnapshotNoOfChunksPerBatch(){
        return 10;
    }
}
