package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.grpc.cluster.Entry;

/**
 * @author Marc Gathier
 */
public class TermIndex {
    private final long term;
    private final long index;

    public TermIndex() {
        this(0,0);
    }

    public TermIndex(Entry e){
        this(e.getTerm(), e.getIndex());
    }

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
