package io.axoniq.axonserver.enterprise.cluster;

/**
 * @author Marc Gathier
 */
public interface RaftLeaderProvider {

    String getLeader(String context);

    boolean isLeader(String context);

    String getLeaderOrWait(String context, boolean aBoolean);
}
