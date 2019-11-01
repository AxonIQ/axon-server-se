package io.axoniq.axonserver.enterprise.jpa;

import io.axoniq.axonserver.grpc.tasks.Status;

import javax.persistence.Embeddable;

/**
 * Registers how to handle errors on execution of tasks.
 *
 * @author Marc Gathier
 * @since 4.3
 */
@Embeddable
public class OnError {

    private Long rescheduleInterval;

    private Status statusOnError;

    public Long getRescheduleInterval() {
        return rescheduleInterval;
    }

    public void setRescheduleInterval(Long rescheduleInterval) {
        this.rescheduleInterval = rescheduleInterval;
    }

    public Status getStatusOnError() {
        return statusOnError;
    }

    public void setStatusOnError(Status statusOnError) {
        this.statusOnError = statusOnError;
    }
}
