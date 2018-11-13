package io.axoniq.axonserver.cluster;

public interface ElectionResult {

    boolean electedMaster();

    int highestTerm();
}
