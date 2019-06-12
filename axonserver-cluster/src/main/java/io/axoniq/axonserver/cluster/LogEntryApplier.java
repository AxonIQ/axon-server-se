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
 * basis. If there is an error applying log entries, they are retried with smaller frequency.
 *
 * @author Milan Savic
 * @since 4.1.5
 */
class LogEntryApplier {

    private static final int MAX_DELAY_MILLIS = 60_000;

    private static final Logger logger = LoggerFactory.getLogger(LogEntryApplier.class);

    private final RaftGroup raftGroup;
    private final Scheduler scheduler;
    private final List<Consumer<Entry>> logEntryConsumers = new CopyOnWriteArrayList<>();
    private final Consumer<Entry> logEntryAppliedConsumer;
    private final AtomicReference<ScheduledRegistration> applyTaskRef = new AtomicReference<>();
    private final AtomicReference<Exception> lastError = new AtomicReference<>();

    /**
     * Initializes Log Entry Applier.
     *
     * @param raftGroup               the RAFT group
     * @param scheduler               used to schedule applies
     * @param logEntryAppliedConsumer invoked when all log entry consumers have applied the entry
     */
    LogEntryApplier(RaftGroup raftGroup, Scheduler scheduler,
                    Consumer<Entry> logEntryAppliedConsumer) {
        this.raftGroup = raftGroup;
        this.scheduler = scheduler;
        this.logEntryAppliedConsumer = logEntryAppliedConsumer;
    }

    /**
     * Starts applying log entries.
     */
    void start() {
        schedule(1);
    }

    /**
     * Stops applying log entries.
     */
    void stop() {
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
    Runnable registerLogEntryConsumer(Consumer<Entry> logEntryConsumer) {
        this.logEntryConsumers.add(logEntryConsumer);
        return () -> this.logEntryConsumers.remove(logEntryConsumer);
    }

    private void schedule(long delayMillis) {
        ScheduledRegistration applyTask =
                scheduler.schedule(() -> raftGroup.logEntryProcessor()
                                                  .apply(raftGroup.localLogEntryStore()::createIterator,
                                                         this::applyLogEntryConsumers),
                                   delayMillis,
                                   TimeUnit.MILLISECONDS);
        applyTaskRef.set(applyTask);
    }

    private void reschedule(long delayMillis) {
        stop();
        if (delayMillis >= MAX_DELAY_MILLIS) {
            logger.warn("{} in term {}: Entry cannot be applied. Error: {}.",
                        groupId(),
                        currentTerm(),
                        lastError.get().getMessage());
            schedule(MAX_DELAY_MILLIS);
        } else {
            schedule(delayMillis);
        }
    }

    private void restart() {
        stop();
        start();
    }

    private void applyLogEntryConsumers(Entry e) {
        logger.trace("{} in term {}: apply {}", groupId(), currentTerm(), e.getIndex());
        for (Consumer<Entry> consumer : logEntryConsumers) {
            try {
                consumer.accept(e);
            } catch (Exception ex) {
                lastError.set(ex);
                long newSchedule = 2 * applyTaskRef.get().getDelay(TimeUnit.MILLISECONDS);
                logger.warn("{} in term {}: Error while applying entry {}. Rescheduling in {}ms.",
                            groupId(),
                            currentTerm(),
                            e.getIndex(),
                            newSchedule,
                            ex);
                reschedule(newSchedule);
                return;
            }
        }
        restart();
        logEntryAppliedConsumer.accept(e);
    }

    private String groupId() {
        return raftGroup.raftConfiguration().groupId();
    }

    private long currentTerm() {
        return raftGroup.localElectionStore().currentTerm();
    }
}
