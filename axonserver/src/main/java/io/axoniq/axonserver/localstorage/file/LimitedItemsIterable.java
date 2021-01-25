package io.axoniq.axonserver.localstorage.file;

import java.util.Iterator;
import javax.annotation.Nonnull;

/**
 * @author Sara Pellegrini
 * @since
 */
public class LimitedItemsIterable<I> implements Iterable<I> {

    private final long limit;
    private final Iterable<I> iterable;

    public LimitedItemsIterable(long limit, Iterable<I> iterable) {
        this.limit = limit;
        this.iterable = iterable;
    }

    @Nonnull
    @Override
    public Iterator<I> iterator() {
        return new LimitedIterator(iterable.iterator());
    }

    private class LimitedIterator implements Iterator<I> {

        private final Iterator<I> iterator;
        private long count = 0;

        private LimitedIterator(Iterator<I> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return count < limit && iterator.hasNext();
        }

        @Override
        public I next() {
            count++;
            return iterator.next();
        }
    }
}
