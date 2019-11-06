package io.axoniq.axonserver.enterprise.jpa;

import io.axoniq.axonserver.grpc.tasks.ScheduleTask;
import io.axoniq.axonserver.grpc.tasks.Status;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
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
    @GeneratedValue
    private Long id;

    private String taskId;

    private String taskExecutor;

    private Payload payload;

    private Long timestamp;

    private Status status;

    private Long rescheduleInterval;

    public Task() {
    }

    public Task(ScheduleTask scheduleTask) {
        taskId = scheduleTask.getTaskId();
        taskExecutor = scheduleTask.getTaskExecutor();
        payload = new Payload(scheduleTask.getPayload());
        timestamp = scheduleTask.getInstant();
        status = Status.SCHEDULED;
        this.rescheduleInterval = scheduleTask.getRescheduleAfter() > 0 ? scheduleTask.getRescheduleAfter() : 1000;

    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
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

    public Long getRescheduleInterval() {
        return rescheduleInterval;
    }

    public void setRescheduleInterval(Long rescheduleInterval) {
        this.rescheduleInterval = rescheduleInterval;
    }

    public ScheduleTask asScheduleTask() {
        ScheduleTask.Builder scheduleTask = ScheduleTask.newBuilder()
                                                        .setInstant(timestamp)
                                                        .setTaskExecutor(taskExecutor)
                                                        .setTaskId(taskId);
        if (payload != null) {
            scheduleTask.setPayload(payload.asSerializedTask());
        }

        scheduleTask.setRescheduleAfter(rescheduleInterval);
        return scheduleTask.build();
    }
}
