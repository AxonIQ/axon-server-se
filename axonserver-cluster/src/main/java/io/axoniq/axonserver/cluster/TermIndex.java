package io.axoniq.axonserver.cluster;

/**
 * Author: marc
 */
public class TermIndex {
    private final long term;
    private final long index;

    public TermIndex(long term, long index) {
        this.term = term;
        this.index = index;
    }

    public long getTerm() {
        return term;
    }

    public long getIndex() {
        return index;
    }
}
