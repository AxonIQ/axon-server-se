package io.axoniq.axonserver.enterprise.replication;

import java.util.Set;

/**
 * @author Marc Gathier
 */
public interface ContextLeaderProvider {

    String getLeader(String context);

    boolean isLeader(String context);

    String getLeaderOrWait(String context, boolean aBoolean);
}
