package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.replication.EntryIterator;
import io.axoniq.axonserver.grpc.cluster.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author Marc Gathier
 */
public class LogEntryProcessor {
    private static final Logger logger = LoggerFactory.getLogger(LogEntryProcessor.class);
    private final AtomicBoolean applyRunning = new AtomicBoolean(false);
    private final ProcessorStore processorStore;
    private final List<Consumer<Entry>> logAppliedListeners = new CopyOnWriteArrayList<>();

    public LogEntryProcessor(ProcessorStore processorStore) {
        this.processorStore = processorStore;
    }

    public void apply(Function<Long, EntryIterator> entryIteratorSupplier, Consumer<Entry> consumer) {
        try {
            int retries = 3;
            while (retries > 0) {
                int applied = applyEntries(entryIteratorSupplier, consumer);
                if (applied > 0) {
                    retries = 0;
                } else {
                    retries--;
                }
            }
        } catch( Exception ex) {
            logger.warn("Apply task failed - {}", ex.getMessage());
        }
    }

    public int applyEntries(Function<Long, EntryIterator> entryIteratorSupplier, Consumer<Entry> consumer) {
        int count = 0;
        if( applyRunning.compareAndSet(false, true)) {
            try {
                if (processorStore.lastAppliedIndex() < processorStore.commitIndex()) {
                    logger.trace("Start to apply entries at: {}", processorStore.lastAppliedIndex());
                    try (EntryIterator iterator = entryIteratorSupplier.apply(processorStore.lastAppliedIndex() + 1)) {
                        boolean beforeCommit = true;
                        while (beforeCommit && iterator.hasNext()) {
                            Entry entry = iterator.next();
                            beforeCommit = entry.getIndex() <= processorStore.commitIndex();
                            if (beforeCommit) {
                                consumer.accept(entry);
                                processorStore.updateLastApplied(entry.getIndex(), entry.getTerm());
                                count++;
                                logAppliedListeners.forEach(listener -> listener.accept(entry));
                            }
                        }
                    }
                    logger.trace("Done apply entries at: {}", processorStore.lastAppliedIndex());
                }
            } finally {
                applyRunning.set(false);
            }
        }
        return count;
    }

    public void markCommitted(long committedIndex, long term) {
        if( committedIndex > processorStore.commitIndex()) {
            processorStore.updateCommit(committedIndex, term);
        }
    }

    public long commitIndex() {
        return processorStore.commitIndex();
    }

    public long commitTerm() {
        return processorStore.commitTerm();
    }

    public long lastAppliedIndex() {
        return processorStore.lastAppliedIndex();
    }

    public long lastAppliedTerm() {
        return processorStore.lastAppliedTerm();
    }

    public void updateLastApplied(long index, long term) {
        processorStore.updateLastApplied(index, term);
    }

    public Registration registerLogAppliedListener(Consumer<Entry> listener){
        this.logAppliedListeners.add(listener);
        return () -> this.logAppliedListeners.remove(listener);
    }

    /**
     * Checks if the index and term match the last applied.
     *
     * @param index the index
     * @param term the term
     * @return true if both index and term match with the one from last applied entry, false otherwise
     */
    public boolean isLastApplied(long index, long term){
        return index == lastAppliedIndex() && term == lastAppliedTerm();
    }

    public void reset() {
        processorStore.updateCommit(0, 0);
        processorStore.updateLastApplied(0, 0);
    }
}
