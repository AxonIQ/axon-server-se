package io.axoniq.axonserver.enterprise.event;

import io.axoniq.axonserver.enterprise.taskscheduler.TaskPublisher;
import io.axoniq.axonserver.grpc.AxonServerClientService;
import io.axoniq.axonserver.grpc.ContextProvider;
import io.axoniq.axonserver.grpc.GrpcExceptionBuilder;
import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.CancelScheduledEventRequest;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventSchedulerGrpc;
import io.axoniq.axonserver.grpc.event.RescheduleEventRequest;
import io.axoniq.axonserver.grpc.event.ScheduleEventRequest;
import io.axoniq.axonserver.grpc.event.ScheduleToken;
import io.axoniq.axonserver.message.event.ScheduledEventExecutor;
import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Service;

/**
 * Implementation of the {@link io.axoniq.axonserver.message.event.EventSchedulerService}, uses Raft to
 * replicate the schedules to all nodes in a context.
 *
 * @author Marc Gathier
 * @since 4.4
 */
@Service
public class EventSchedulerService extends EventSchedulerGrpc.EventSchedulerImplBase
        implements AxonServerClientService {

    private final ContextProvider contextProvider;
    private final TaskPublisher taskPublisher;

    /**
     * @param contextProvider component that returns the context for a specific request.
     * @param taskPublisher   factory to get a {@link io.axoniq.axonserver.enterprise.cluster.RaftGroupService}
     *                        instance for a context
     */
    public EventSchedulerService(ContextProvider contextProvider,
                                 TaskPublisher taskPublisher) {
        this.contextProvider = contextProvider;
        this.taskPublisher = taskPublisher;
    }

    /**
     * Schedules the publication of an event
     *
     * @param request          contains the event and the timestamp on which to publish the event
     * @param responseObserver gets the taskId of the scheduled action
     */
    @Override
    public void scheduleEvent(ScheduleEventRequest request, StreamObserver<ScheduleToken> responseObserver) {
        String context = contextProvider.getContext();
        doScheduleEvent(request.getEvent(), request.getInstant(), responseObserver, context);
    }

    private void doScheduleEvent(Event event, long instant, StreamObserver<ScheduleToken> responseObserver,
                                 String context) {
        taskPublisher.publishScheduledTask(context, ScheduledEventExecutor.class.getName(), serialize(event), instant)
                     .thenApply(taskId -> {
                         responseObserver.onNext(ScheduleToken.newBuilder()
                                                              .setToken(taskId)
                                                              .build());
                         responseObserver.onCompleted();
                         return null;
                     })
                     .exceptionally(ex -> {
                         responseObserver.onError(GrpcExceptionBuilder.build(ex));
                         return null;
                     });
    }

    private SerializedObject serialize(Event event) {
        return SerializedObject.newBuilder()
                               .setType("ScheduledEvent")
                               .setData(event.toByteString())
                               .build();
    }

    /**
     * Cancels a scheduled event and schedules another in its place.
     *
     * @param request          request containing the new event, timestamp and optionally the token of the event to
     *                         cancel
     * @param responseObserver gets the taskId of the new scheduled action
     */
    @Override
    public void rescheduleEvent(RescheduleEventRequest request, StreamObserver<ScheduleToken> responseObserver) {
        String context = contextProvider.getContext();
        if (!"".equals(request.getToken())) {
            taskPublisher.cancelScheduledTask(context, request.getToken())
                         .thenAccept(r -> doScheduleEvent(request.getEvent(),
                                                          request.getInstant(),
                                                          responseObserver,
                                                          context))
                         .exceptionally(ex -> {
                             responseObserver.onError(GrpcExceptionBuilder.build(ex));
                             return null;
                         });
        } else {
            doScheduleEvent(request.getEvent(), request.getInstant(), responseObserver, context);
        }
    }

    /**
     * Cancels a scheduled request. If the request is already completed, this is a no-op.
     *
     * @param request          contains the token of the scheduled event to cancel
     * @param responseObserver gets acknowledgement of the cancellation
     */
    @Override
    public void cancelScheduledEvent(CancelScheduledEventRequest request,
                                     StreamObserver<InstructionAck> responseObserver) {
        taskPublisher.cancelScheduledTask(contextProvider.getContext(), request.getToken())
                     .whenComplete((r, ex) -> {
                         if (ex == null) {
                             responseObserver.onNext(InstructionAck.newBuilder()
                                                                   .setSuccess(true)
                                                                   .build());
                             responseObserver.onCompleted();
                         } else {
                             responseObserver.onError(GrpcExceptionBuilder.build(ex));
                         }
                     });
    }
}
