package io.axoniq.axonserver.enterprise.jpa;

import io.axoniq.axonserver.grpc.tasks.ScheduleTask;
import io.axoniq.axonserver.grpc.tasks.Status;

import java.util.Optional;
import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * Scheduled task to be run by the admin leader.
 *
 * @author Marc Gathier
 * @since 4.3
 */
@Entity
public class Task {

    @Id
    private String taskId;

    private String taskExecutor;

    private Payload payload;

    private Long timestamp;

    private Status status;

    private Long retryInterval;

    private String context;

    public Task() {
    }

    public Task(String context, ScheduleTask scheduleTask) {
        taskId = scheduleTask.getTaskId();
        taskExecutor = scheduleTask.getTaskExecutor();
        payload = new Payload(scheduleTask.getPayload());
        timestamp = scheduleTask.getInstant();
        status = Status.SCHEDULED;
        this.context = context;
        this.retryInterval = scheduleTask.getRetryInterval() > 0 ? scheduleTask.getRetryInterval() : 1000;

    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getTaskExecutor() {
        return taskExecutor;
    }

    public void setTaskExecutor(String taskExecutor) {
        this.taskExecutor = taskExecutor;
    }

    public Payload getPayload() {
        return payload;
    }

    public void setPayload(Payload payload) {
        this.payload = payload;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public Long getRetryInterval() {
        return Optional.ofNullable(retryInterval).orElse(1000L);
    }

    public void setRetryInterval(Long retryInterval) {
        this.retryInterval = retryInterval;
    }

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public ScheduleTask asScheduleTask() {
        ScheduleTask.Builder scheduleTask = ScheduleTask.newBuilder()
                                                        .setInstant(timestamp)
                                                        .setTaskExecutor(taskExecutor)
                                                        .setTaskId(taskId)
                                                        .setRetryInterval(retryInterval);

        if (payload != null) {
            scheduleTask.setPayload(payload.asSerializedTask());
        }

        return scheduleTask.build();
    }
}
