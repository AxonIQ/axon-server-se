package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.scheduler.ScheduledRegistration;
import io.axoniq.axonserver.cluster.scheduler.Scheduler;
import io.axoniq.axonserver.grpc.cluster.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Takes log entries from the {@link io.axoniq.axonserver.cluster.replication.LogEntryStore} and applies them on regular
 * basis.
 *
 * @author Milan Savic
 * @since 4.1.5
 */
public class LogEntryApplier {

    private static final Logger logger = LoggerFactory.getLogger(LogEntryApplier.class);

    private final RaftGroup raftGroup;
    private final Scheduler scheduler;
    private final List<Consumer<Entry>> logEntryConsumers = new CopyOnWriteArrayList<>();
    private final Consumer<Entry> logEntryAppliedConsumer;
    private final AtomicReference<ScheduledRegistration> applyTaskRef = new AtomicReference<>();

    /**
     * Initializes Log Entry Applier.
     *
     * @param raftGroup               the RAFT group
     * @param scheduler               used to schedule applies
     * @param logEntryAppliedConsumer invoked when all log entry consumers have applied the entry
     */
    public LogEntryApplier(RaftGroup raftGroup, Scheduler scheduler,
                           Consumer<Entry> logEntryAppliedConsumer) {
        this.raftGroup = raftGroup;
        this.scheduler = scheduler;
        this.logEntryAppliedConsumer = logEntryAppliedConsumer;
    }

    /**
     * Starts applying log entries.
     */
    public void start() {
        schedule(0, 1, TimeUnit.MILLISECONDS);
    }

    /**
     * Stops applying log entries.
     */
    public void stop() {
        ScheduledRegistration applyTask = applyTaskRef.getAndSet(null);
        if (applyTask != null) {
            applyTask.cancel(true);
        }
    }

    /**
     * Registers a consumer for committed log entries.
     *
     * @param logEntryConsumer to consume committed log entries
     * @return a Runnable to be invoked in order to cancel this registration
     */
    public Runnable registerLogEntryConsumer(Consumer<Entry> logEntryConsumer) {
        this.logEntryConsumers.add(logEntryConsumer);
        return () -> this.logEntryConsumers.remove(logEntryConsumer);
    }

    private void schedule(long initialDelay, long delay, TimeUnit timeUnit) {
        ScheduledRegistration applyTask = scheduler.scheduleWithFixedDelay(() -> raftGroup
                                                                                   .logEntryProcessor()
                                                                                   .apply(raftGroup.localLogEntryStore()::createIterator,
                                                                                          this::applyEntryConsumers),
                                                                           initialDelay,
                                                                           delay,
                                                                           TimeUnit.MILLISECONDS);
        applyTaskRef.set(applyTask);
    }

    private void applyEntryConsumers(Entry e) {
        logger.trace("{} in term {}: apply {}", groupId(), currentTerm(), e.getIndex());
        logEntryConsumers.forEach(consumer -> {
            try {
                consumer.accept(e);
            } catch (Exception ex) {
                logger.warn("{} in term {}: Error while applying entry {}",
                            groupId(),
                            currentTerm(),
                            e.getIndex(),
                            ex);
            }
        });
        logEntryAppliedConsumer.accept(e);
    }

    private String groupId() {
        return raftGroup.raftConfiguration().groupId();
    }

    private long currentTerm() {
        return raftGroup.localElectionStore().currentTerm();
    }
}
