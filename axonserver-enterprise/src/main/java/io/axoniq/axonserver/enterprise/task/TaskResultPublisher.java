package io.axoniq.axonserver.enterprise.task;

import io.axoniq.axonserver.grpc.tasks.Status;

/**
 * @author Marc Gathier
 */
public interface TaskResultPublisher {

    void publishResult(String taskId, Status status, Long newSchedule, long retry);
}
