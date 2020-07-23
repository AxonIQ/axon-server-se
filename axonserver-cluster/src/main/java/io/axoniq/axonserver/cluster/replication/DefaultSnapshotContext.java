package io.axoniq.axonserver.cluster.replication;

import io.axoniq.axonserver.cluster.snapshot.SnapshotContext;
import io.axoniq.axonserver.cluster.util.RoleUtils;
import io.axoniq.axonserver.grpc.cluster.AppendEntryFailure;
import io.axoniq.axonserver.grpc.cluster.Role;

import java.util.Map;

/**
 * {@link SnapshotContext} implementation that takes the data boundaries from the {@link AppendEntryFailure}.
 *
 * @author Sara Pellegrini
 * @since 4.1
 */
public class DefaultSnapshotContext implements SnapshotContext {

    private final boolean eventStore;
    private final Role role;
    private final Map<String, Long> lastEventTokenPerContextMap;
    private final Map<String, Long> lastSnapshoptTokenPerContextMap;
    private final boolean peerSupportsReplicationGroups;

    /**
     * Constructor
     *
     * @param lastEventTokenPerContextMap     map of last tokens per context from the remote peer
     * @param lastSnapshoptTokenPerContextMap map of last snapshot tokens per context from the remote peer
     * @param peerSupportsReplicationGroups   indicates if the peer supports replication groups (version >= 4.4)
     * @param role                            the role of the remote peer in the replication group
     */

    public DefaultSnapshotContext(Map<String, Long> lastEventTokenPerContextMap,
                                  Map<String, Long> lastSnapshoptTokenPerContextMap,
                                  boolean peerSupportsReplicationGroups,
                                  Role role) {
        this.lastEventTokenPerContextMap = lastEventTokenPerContextMap;
        this.lastSnapshoptTokenPerContextMap = lastSnapshoptTokenPerContextMap;
        this.peerSupportsReplicationGroups = peerSupportsReplicationGroups;
        this.eventStore = RoleUtils.hasStorage(role);
        this.role = role;
    }

    /**
     * Returns the first event token to include in a install snapshot sequence. If the target node is not an event store
     * the operation returns Long.MAX_VALUE to prevent any events being included in the snapshot.
     *
     * @return first event token to include in snapshot
     */
    @Override
    public long fromEventSequence(String context) {
        if (!eventStore) {
            return Long.MAX_VALUE;
        }
        return lastEventTokenPerContextMap.getOrDefault(context, -1L) + 1;
    }

    /**
     * Returns the first aggregate snapshot token to include in a install snapshot sequence. If the target node is not
     * an event store
     * the operation returns Long.MAX_VALUE to prevent any aggregate snapshots being included in the snapshot.
     *
     * @return first snapshot token to include in snapshot
     */
    @Override
    public long fromSnapshotSequence(String context) {
        if (!eventStore) {
            return Long.MAX_VALUE;
        }
        return lastSnapshoptTokenPerContextMap.getOrDefault(context, -1L) + 1;
    }

    @Override
    public boolean supportsReplicationGroups() {
        return peerSupportsReplicationGroups;
    }

    @Override
    public Role role() {
        return role;
    }

    @Override
    public String toString() {
        return "DefaultSnapshotContext[fromEvent=" + lastEventTokenPerContextMap
                + ",fromSnapshot=" + lastSnapshoptTokenPerContextMap + "]";
    }
}
