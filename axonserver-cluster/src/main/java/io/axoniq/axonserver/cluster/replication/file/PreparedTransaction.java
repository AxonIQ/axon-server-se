package io.axoniq.axonserver.cluster.replication.file;

/**
 * Author: marc
 */
public class PreparedTransaction {

    private final WritePosition claim;
    private final byte[] transformedData;

    public PreparedTransaction(WritePosition claim, byte[] transformedData) {
        this.claim = claim;
        this.transformedData = transformedData;
    }

    public WritePosition getClaim() {
        return claim;
    }

    public byte[] getTransformedData() {
        return transformedData;
    }
}
