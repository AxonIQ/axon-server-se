package io.axoniq.axonserver.enterprise.cluster;

import java.util.Set;

/**
 * @author Marc Gathier
 */
public interface RaftLeaderProvider {

    String getLeader(String context);

    boolean isLeader(String context);

    String getLeaderOrWait(String context, boolean aBoolean);

    Set<String> leaderFor();
}
