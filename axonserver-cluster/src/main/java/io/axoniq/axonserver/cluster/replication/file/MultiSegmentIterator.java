package io.axoniq.axonserver.cluster.replication.file;

import io.axoniq.axonserver.cluster.TermIndex;
import io.axoniq.axonserver.cluster.replication.EntryIterator;
import io.axoniq.axonserver.grpc.cluster.Entry;
import org.springframework.data.util.CloseableIterator;

import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * @author Marc Gathier
 * @since 4.1
 */
public class MultiSegmentIterator implements EntryIterator {

    private final Function<Long, CloseableIterator<Entry>> iteratorProvider;
    private final AtomicLong nextIndex = new AtomicLong();

    private volatile Entry previous;
    private volatile Entry next;
    private volatile CloseableIterator<Entry> iterator;

    public MultiSegmentIterator(Function<Long, CloseableIterator<Entry>> iteratorProvider,
                                long nextIndex) {
        this.nextIndex.set(nextIndex);
        this.iteratorProvider = iteratorProvider;

        iterator = iteratorProvider.apply(nextIndex - 1);
        if (iterator != null && iterator.hasNext()) {
            previous = iterator.next();
        } else {
            iterator = iteratorProvider.apply(nextIndex);
        }
    }


    @Override
    public boolean hasNext() {
        if (iterator == null) {
            return false;
        }
        if (iterator.hasNext()) {
            return true;
        }

        close();
        iterator = iteratorProvider.apply(nextIndex.get());
        return hasNext();
    }

    @Override
    public Entry next() {
        if (iterator == null || !iterator.hasNext()) {
            throw new NoSuchElementException();
        }
        if (next != null) {
            previous = next;
        }
        next = iterator.next();
        nextIndex.getAndIncrement();
        return next;
    }

    @Override
    public TermIndex previous() {
        if (previous == null) {
            return new TermIndex(0,0);
        }
        return new TermIndex(previous.getTerm(), previous.getIndex());
    }

    @Override
    public void close() {
        if (iterator != null) {
            iterator.close();
        }
    }

}
