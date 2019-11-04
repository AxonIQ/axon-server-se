package io.axoniq.axonserver.enterprise.jpa;

import io.axoniq.axonserver.grpc.tasks.ErrorHandler;
import io.axoniq.axonserver.grpc.tasks.Status;

import java.util.Optional;
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

    public OnError() {
    }

    public OnError(ErrorHandler errorHandler) {
        this.rescheduleInterval = errorHandler.getRescheduleAfter() > 0 ? errorHandler.getRescheduleAfter() : 1000;
        this.statusOnError = errorHandler.getStatus();
    }

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

    public ErrorHandler asErrorHandler() {
        return ErrorHandler.newBuilder()
                           .setRescheduleAfter(rescheduleInterval)
                           .setStatus(Optional.ofNullable(statusOnError).orElse(Status.FAILED))
                           .build();
    }
}
