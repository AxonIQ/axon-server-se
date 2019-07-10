package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.exception.LogEntryApplyException;
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
    private volatile String lastError;

    public LogEntryProcessor(ProcessorStore processorStore) {
        this.processorStore = processorStore;
    }

    public void apply(Function<Long, EntryIterator> entryIteratorSupplier, Consumer<Entry> consumer) {
        if (applyRunning.compareAndSet(false, true)) {
            try {
                if (processorStore.lastAppliedIndex() < processorStore.commitIndex()) {
                    logger.trace("Start to apply entries at: {}", processorStore.lastAppliedIndex());
                    try (EntryIterator iterator = entryIteratorSupplier.apply(
                            processorStore.lastAppliedIndex() + 1)) {
                        boolean beforeCommit = true;
                        while (beforeCommit && iterator.hasNext()) {
                            Entry entry = iterator.next();
                            beforeCommit = entry.getIndex() <= processorStore.commitIndex();
                            if (beforeCommit) {
                                consumer.accept(entry);
                                processorStore.updateLastApplied(entry.getIndex(), entry.getTerm());
                                logAppliedListeners.forEach(listener -> listener.accept(entry));
                            }
                        }
                    }
                    logger.trace("Done apply entries at: {}", processorStore.lastAppliedIndex());
                    lastError = null;
                }
            } catch (LogEntryApplyException ex) {
                // Already logged
            } catch (Exception ex) {
                String error = String.format("%s: Apply failed last applied : %d, commitIndex: %d, %s",
                                             processorStore.groupId(),
                                             processorStore.lastAppliedIndex(),
                                             processorStore.commitIndex(),
                                             ex.getMessage());

                if (!error.equals(lastError)) {
                    lastError = error;
                    logger.error(lastError);
                }
            } finally {
                applyRunning.set(false);
            }
        }
    }

    public void markCommitted(long committedIndex, long term) {
        if (committedIndex > processorStore.commitIndex()) {
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

    public Registration registerLogAppliedListener(Consumer<Entry> listener) {
        this.logAppliedListeners.add(listener);
        return () -> this.logAppliedListeners.remove(listener);
    }

    /**
     * Checks if the index and term match the last applied.
     *
     * @param index the index
     * @param term  the term
     * @return true if both index and term match with the one from last applied entry, false otherwise
     */
    public boolean isLastApplied(long index, long term) {
        return index == lastAppliedIndex() && term == lastAppliedTerm();
    }


    /**
     * Reset pointers of the log entry processor when applied entry is higher than last log entry. In this case we want
     * to start from a snapshot again.
     */
    public void reset() {
        processorStore.updateCommit(0, 0);
        processorStore.updateLastApplied(0, 0);
    }
}
