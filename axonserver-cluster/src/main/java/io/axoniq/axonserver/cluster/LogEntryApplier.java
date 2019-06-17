package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.scheduler.ScheduledRegistration;
import io.axoniq.axonserver.cluster.scheduler.Scheduler;
import io.axoniq.axonserver.grpc.cluster.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.lang.Math.min;

/**
 * Takes log entries from the {@link io.axoniq.axonserver.cluster.replication.LogEntryStore} and applies them on regular
 * basis. If there is an error applying log entries, they are retried with smaller frequency.
 *
 * @author Milan Savic
 * @since 4.1.5
 */
class LogEntryApplier {

    private static final int MAX_DELAY_MILLIS = 60_000;

    private AtomicLong currentDelayMillis = new AtomicLong(0);

    private static final Logger logger = LoggerFactory.getLogger(LogEntryApplier.class);

    private final RaftGroup raftGroup;
    private final Scheduler scheduler;
    private final Map<String, LogEntryConsumer> logEntryConsumers = new ConcurrentHashMap<>();
    private final Consumer<Entry> logEntryAppliedConsumer;
    private final AtomicReference<ScheduledRegistration> applyTaskRef = new AtomicReference<>();
    private final AtomicReference<Exception> lastError = new AtomicReference<>();
    private final NewConfigurationConsumer newConfigurationConsumer;

    /**
     * Initializes Log Entry Applier.
     *
     * @param raftGroup                the RAFT group
     * @param scheduler                used to schedule applies
     * @param logEntryAppliedConsumer  invoked when all log entry consumers have applied the entry
     * @param newConfigurationConsumer consumes new configuration
     */
    LogEntryApplier(RaftGroup raftGroup, Scheduler scheduler, Consumer<Entry> logEntryAppliedConsumer,
                    NewConfigurationConsumer newConfigurationConsumer) {
        this.raftGroup = raftGroup;
        this.scheduler = scheduler;
        this.logEntryAppliedConsumer = logEntryAppliedConsumer;
        this.newConfigurationConsumer = newConfigurationConsumer;
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
    Runnable registerLogEntryConsumer(LogEntryConsumer logEntryConsumer) {
        this.logEntryConsumers.put(logEntryConsumer.entryType(), logEntryConsumer);
        return () -> this.logEntryConsumers.remove(logEntryConsumer.entryType());
    }

    private void schedule(long delayMillis) {
        currentDelayMillis.set(delayMillis);
        ScheduledRegistration applyTask =
                scheduler.scheduleWithFixedDelay(() -> raftGroup.logEntryProcessor()
                                                                .apply(raftGroup.localLogEntryStore()::createIterator,
                                                                       this::applyLogEntryConsumers),
                                                 delayMillis,
                                                 delayMillis,
                                                 TimeUnit.MILLISECONDS);
        applyTaskRef.set(applyTask);
    }

    private void reschedule(long delayMillis) {
        long toBeScheduled = min(delayMillis, MAX_DELAY_MILLIS);
        if (currentDelayMillis.get() == toBeScheduled) {
            return;
        }
        stop();
        schedule(toBeScheduled);
    }

    private void applyLogEntryConsumers(Entry e) {
        logger.trace("{} in term {}: apply {}", groupId(), currentTerm(), e.getIndex());
        // TODO: 6/12/2019 start a transaction?
        try {
            if (e.hasNewConfiguration()) {
                logger.info("{} in term {}: Received new configuration {}.",
                            groupId(),
                            currentTerm(),
                            e.getNewConfiguration());
                newConfigurationConsumer.consume(e.getNewConfiguration());
            }
            if (e.hasSerializedObject()) {
                String entryType = e.getSerializedObject().getType();
                if (logEntryConsumers.containsKey(entryType)) {
                    logEntryConsumers.get(entryType).consumeLogEntry(groupId(), e);
                    lastError.set(null);
                } else {
                    logger.warn("{} in term {}: There is no log entry processor for {} entry type.",
                                groupId(),
                                currentTerm(),
                                entryType);
                }
            }
        } catch (Exception ex) {
            lastError.set(ex);
            long newSchedule = 2 * currentDelayMillis.get();
            logger.warn("{} in term {}: Error while applying entry {}. Rescheduling in {}ms.",
                        groupId(),
                        currentTerm(),
                        e.getIndex(),
                        newSchedule,
                        ex);
            reschedule(newSchedule);
            // TODO: 6/12/2019 rollback transaction?
            throw new RuntimeException("Failed to apply entry", ex);
        }
        // TODO: 6/12/2019 commit transaction?
        reschedule(1);
        logEntryAppliedConsumer.accept(e);
    }

    private String groupId() {
        return raftGroup.raftConfiguration().groupId();
    }

    private long currentTerm() {
        return raftGroup.localElectionStore().currentTerm();
    }
}
