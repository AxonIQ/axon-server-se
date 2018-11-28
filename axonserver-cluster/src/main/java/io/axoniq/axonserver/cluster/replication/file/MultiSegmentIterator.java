package io.axoniq.axonserver.cluster.replication.file;

import io.axoniq.axonserver.cluster.TermIndex;
import io.axoniq.axonserver.cluster.replication.EntryIterator;
import io.axoniq.axonserver.grpc.cluster.Entry;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Author: marc
 */
public class MultiSegmentIterator extends EntryIterator {

    private final PrimaryEventStore primaryEventStore;
    private final AtomicLong nextIndex = new AtomicLong();

    private volatile Entry previous;
    private volatile Entry next;
    private volatile SegmentEntryIterator iterator;

    public MultiSegmentIterator(PrimaryEventStore primaryEventStore,
                                long nextIndex) {
        super(null, nextIndex);
        this.nextIndex.set(nextIndex);
        this.primaryEventStore = primaryEventStore;
        iterator = primaryEventStore.getIterator(nextIndex);
    }


    @Override
    public boolean hasNext() {
        if( iterator == null ) return false;
        if( iterator.hasNext()) {
            previous = next;
            next = iterator.next();
            nextIndex.incrementAndGet();
        } else {
            iterator.close();
            iterator = primaryEventStore.getIterator(nextIndex.get());
            return hasNext();
        }
        return next != null;
    }

    @Override
    public Entry next() {
        return next;
    }

    @Override
    public TermIndex previous() {
        if( previous == null) {
            if (next != null && next.getIndex() > 1) {
                previous = primaryEventStore.getEntry(next.getIndex() - 1);
            }
        }
        if( previous == null) {
            return new TermIndex(0,0);
        }
        return new TermIndex(previous.getTerm(), previous.getIndex());
    }

}
