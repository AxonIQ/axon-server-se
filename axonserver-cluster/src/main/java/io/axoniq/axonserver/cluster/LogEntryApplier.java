package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.exception.LogEntryApplyException;
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
    private static final int MAX_NO_WORK_DELAY_MILLIS = 200;
    private static final int MIN_DELAY_MILLIS = 1;
    private static final Logger logger = LoggerFactory.getLogger(LogEntryApplier.class);
    public static final long LONG_PERIOD_WITHOUT_MESSAGES = TimeUnit.MINUTES.toMillis(5);
    public static final long MEDIUM_PERIOD_WITHOUT_MESSAGES = TimeUnit.MINUTES.toMillis(1);
    private final RaftGroup raftGroup;
    private final Scheduler scheduler;
    private final Map<String, LogEntryConsumer> logEntryConsumers = new ConcurrentHashMap<>();
    private final Consumer<Entry> logEntryAppliedConsumer;
    private final AtomicReference<ScheduledRegistration> applyTaskRef = new AtomicReference<>();
    private final AtomicReference<Exception> lastError = new AtomicReference<>();
    private final NewConfigurationConsumer newConfigurationConsumer;
    private AtomicLong currentDelayMillis = new AtomicLong(0);
    private AtomicLong lastApplied = new AtomicLong(0);

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
        schedule(MAX_NO_WORK_DELAY_MILLIS);
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
                scheduler.schedule(this::processorRun,
                                   delayMillis,
                                   TimeUnit.MILLISECONDS);
        applyTaskRef.set(applyTask);
    }

    private void processorRun() {
        switch (raftGroup.logEntryProcessor()
                         .apply(raftGroup.localLogEntryStore()::createIterator,
                                this::applyLogEntryConsumers)) {
            case FAILED:
                long newSchedule = min(MAX_DELAY_MILLIS, 2 * currentDelayMillis.get());
                schedule(newSchedule);
                break;
            case RUNNING:
                break;
            case APPLIED:
                lastApplied.set(System.currentTimeMillis());
                schedule(MIN_DELAY_MILLIS);
                break;
            case NO_WORK:
                schedule(calculateDelay());
                break;
        }
    }

    private long calculateDelay() {
        long timeSinceLastApply = System.currentTimeMillis() - lastApplied.get();
        if (timeSinceLastApply > LONG_PERIOD_WITHOUT_MESSAGES) {
            return MAX_NO_WORK_DELAY_MILLIS;
        }
        if (timeSinceLastApply > MEDIUM_PERIOD_WITHOUT_MESSAGES) {
            return MAX_NO_WORK_DELAY_MILLIS / 10;
        }

        return MIN_DELAY_MILLIS;
    }

    public void applyLogEntryConsumers(Entry e) {
        if (logger.isTraceEnabled()) {
            logger.trace("{} in term {}: apply {}", groupId(), currentTerm(), e.getIndex());
        }

        try {
            if (e.hasNewConfiguration()) {
                logger.info("{} in term {}: Received new configuration {}.",
                            groupId(),
                            currentTerm(),
                            e.getNewConfiguration());
                newConfigurationConsumer.consume(e.getNewConfiguration());
            } else if (e.hasSerializedObject()) {
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
            // TODO: 6/12/2019 rollback transaction?
            throw new LogEntryApplyException("Failed to apply entry", ex);
        }
        logEntryAppliedConsumer.accept(e);
    }

    private String groupId() {
        return raftGroup.raftConfiguration().groupId();
    }

    private long currentTerm() {
        return raftGroup.localElectionStore().currentTerm();
    }
}
