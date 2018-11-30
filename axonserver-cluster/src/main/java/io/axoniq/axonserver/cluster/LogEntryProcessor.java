package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.replication.EntryIterator;
import io.axoniq.axonserver.grpc.cluster.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Author: marc
 */
public class LogEntryProcessor {
    private final Logger logger = LoggerFactory.getLogger(LogEntryProcessor.class);

    private final AtomicLong lastApplied = new AtomicLong(0);
    private final AtomicLong commitIndex = new AtomicLong(0);
    private final AtomicBoolean applyRunning = new AtomicBoolean(false);
    private volatile Thread commitListenerThread;
    private volatile boolean running;


    public void start(Function<Long, EntryIterator> entryIteratorSupplier, Consumer<Entry> consumer) {
        commitListenerThread = Thread.currentThread();
        running = true;
        while (running) {
            int retries = 1;
            while (retries > 0) {
                int applied = applyEntries(entryIteratorSupplier, consumer);
                if (applied > 0) {
                    retries = 0;
                } else {
                    retries--;
                }

                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            }
        }
    }

    public int applyEntries(Function<Long, EntryIterator> entryIteratorSupplier, Consumer<Entry> consumer) {
        int count = 0;
        if( applyRunning.compareAndSet(false, true)) {
            if( lastApplied.get() < commitIndex.get()) {
                try(EntryIterator iterator = entryIteratorSupplier.apply(lastApplied.get() + 1)) {
                    boolean beforeCommit = true;
                    while (beforeCommit && iterator.hasNext()) {
                        Entry entry = iterator.next();
                        beforeCommit = entry.getIndex() <= commitIndex.get();
                        if (beforeCommit) {
                            consumer.accept(entry);
                            lastApplied.incrementAndGet();
                            count++;
                        }
                    }
                }
            }
            applyRunning.set(false);
        }
        return count;
    }

    public void markCommitted(long committedIndex) {
        if( committedIndex > commitIndex.get()) {
            commitIndex.set(committedIndex);
            if( commitListenerThread != null) {
                LockSupport.unpark(commitListenerThread);
            }
        }
    }

    public long commitIndex() {
        return commitIndex.get();
    }

    public long lastAppliedIndex() {
        return lastApplied.get();
    }

    public void registerCommitListener(Thread currentThread) {
        this.commitListenerThread = currentThread;
    }

    public void stop() {
        running = false;
    }
}
