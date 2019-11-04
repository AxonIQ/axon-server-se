package io.axoniq.axonserver.enterprise.jpa;

import io.axoniq.axonserver.grpc.tasks.ScheduleTask;
import io.axoniq.axonserver.grpc.tasks.Status;

import java.util.Optional;
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

    private OnError errorHandler = new OnError();

    private Status status;

    public Task() {
    }

    public Task(ScheduleTask scheduleTask) {
        taskId = scheduleTask.getTaskId();
        taskExecutor = scheduleTask.getTaskExecutor();
        payload = new Payload(scheduleTask.getPayload());
        timestamp = scheduleTask.getInstant();
        status = Status.SCHEDULED;
        errorHandler = new OnError(scheduleTask.getErrorHandler());
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

    public OnError getErrorHandler() {
        return errorHandler;
    }

    public void setErrorHandler(OnError errorHandler) {
        this.errorHandler = errorHandler;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public ScheduleTask asScheduleTask() {
        ScheduleTask.Builder scheduleTask = ScheduleTask.newBuilder()
                                                        .setInstant(timestamp)
                                                        .setTaskExecutor(taskExecutor)
                                                        .setStatus(Optional.of(status).orElse(Status.SCHEDULED))
                                                        .setTaskId(taskId);
        if (payload != null) {
            scheduleTask.setPayload(payload.asSerializedTask());
        }

        if (errorHandler != null) {
            scheduleTask.setErrorHandler(errorHandler.asErrorHandler());
        }
        return scheduleTask.build();
    }
}
