package io.axoniq.axonhub.message;

import io.axoniq.axonhub.exception.ErrorCode;
import io.axoniq.axonhub.exception.MessagingPlatformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Author: marc
 */
public class FlowControlQueues<T> {
    private static final Logger logger = LoggerFactory.getLogger(FlowControlQueues.class);
    private static final AtomicLong requestId = new AtomicLong(0);
    private final Comparator<T> comparator;

    private final Map<String, BlockingQueue<FilterNode>> segments = new ConcurrentHashMap<>();

    public FlowControlQueues(Comparator<T> comparator) {
        this.comparator = comparator;
    }

    public FlowControlQueues() {
        this(null);
    }

    public T take(String filterValue) throws InterruptedException {
        BlockingQueue<FilterNode> filterSegment = segments.computeIfAbsent(filterValue, f -> new PriorityBlockingQueue<>());
        return filterSegment.take().value;
    }

    public void put(String filterValue, T value)  {
        if( value == null) throw new NullPointerException();
        BlockingQueue<FilterNode> filterSegment = segments.computeIfAbsent(filterValue, f -> new PriorityBlockingQueue<>());
        FilterNode filterNode = new FilterNode(value);
        if( !filterSegment.offer(filterNode)) {
            throw new MessagingPlatformException(ErrorCode.OTHER, "Failed to add request to queue " + filterValue);

        }

        if( logger.isTraceEnabled()) {
            filterSegment.forEach(node -> logger.trace("entry: {}", node.id));
        }
    }

    public void move( String oldFilterValue, Function<T, String> newFilterAssignment) {
        logger.debug("Remove: {}", oldFilterValue);
        BlockingQueue<FilterNode> filterSegment = segments.remove(oldFilterValue);
        if( filterSegment == null) return;

        filterSegment.forEach(filterNode -> {
            String newFilterValue = newFilterAssignment.apply(filterNode.value);
            if( newFilterValue != null) {
                try {
                    segments.computeIfAbsent(newFilterValue, f -> new PriorityBlockingQueue<>()).put(filterNode);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.debug("Interrupt during move");
                    throw new MessagingPlatformException(ErrorCode.OTHER, "Failed to add request to queue " + newFilterValue);
                }
            }

        });
    }

    public Map<String, BlockingQueue<FilterNode>> getSegments() {
        return segments;
    }

    public class FilterNode implements Comparable<FilterNode> {
        private final T value;
        private final long id;

        public FilterNode(T value) {
            this.value = value;
            this.id = requestId.getAndIncrement();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FilterNode that = (FilterNode) o;
            return id == that.id;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }

        @Override
        public int compareTo(FilterNode o) {
            if( comparator != null) {
                int rc = comparator.compare(value, o.value);
                if( rc != 0) return rc;

            }
            return Long.compare(id, o.id);


        }
    }

}
