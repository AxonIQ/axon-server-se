package io.axoniq.axonserver.cluster;

/**
 * Result of a single run of the {@link LogEntryProcessor}.
 *
 * @author Marc Gathier
 * @since 4.3
 */
public enum ApplyResult {
    /**
     * Run failed applying the last entry
     */
    FAILED,
    /**
     * The {@link LogEntryProcessor} was already active when the run was started.
     */
    RUNNING,
    /**
     * A number of log entries were successfully applied.
     */
    APPLIED,
    /**
     * There were no entries ready to be appled.
     */
    NO_WORK
}
