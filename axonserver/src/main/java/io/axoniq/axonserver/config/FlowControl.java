package io.axoniq.axonserver.config;

import io.axoniq.axonserver.KeepNames;

/**
 * Author: marc
 */
@KeepNames
public class FlowControl {
    /**
     * Initial number of permits granted in communication between axonserver platform nodes.
     */
    private long initialPermits=100000;
    /**
     * Threshold at which the node will send another grant of newPermits to the connected platform node.
     */
    private long threshold=10000;
    /**
     * Additional number of permits granted in communication between axonserver platform nodes.
     */
    private long newPermits=100000;

    public long getInitialPermits() {
        return initialPermits;
    }

    public void setInitialPermits(long initialPermits) {
        this.initialPermits = initialPermits;
    }

    public long getThreshold() {
        return threshold;
    }

    public void setThreshold(long threshold) {
        this.threshold = threshold;
    }

    public long getNewPermits() {
        return newPermits;
    }

    public void setNewPermits(long newPermits) {
        this.newPermits = newPermits;
    }
}
