package io.axoniq.axonserver.cluster;

public interface ElectionResult {

    boolean electedLeader();

    int highestTerm();
}
