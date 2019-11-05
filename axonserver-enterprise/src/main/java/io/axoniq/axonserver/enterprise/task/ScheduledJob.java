package io.axoniq.axonserver.enterprise.task;

/**
 * @author Marc Gathier
 */
public interface ScheduledJob {

    void execute(Object payload);
}
