package io.axoniq.axonserver.enterprise.replication;

import java.util.Set;

/**
 * @author Marc Gathier
 */
public interface RaftLeaderProvider {

    String getLeader(String replicationGroup);

    boolean isLeader(String replicationGroup);

    Set<String> leaderFor();
}
