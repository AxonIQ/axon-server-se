package io.axoniq.axonserver.cluster;

/**
 * @author Sara Pellegrini
 * @since 4.0
 */
public interface HeartbeatRound {

    void registerResponse(String node);

    boolean isValid();

}
