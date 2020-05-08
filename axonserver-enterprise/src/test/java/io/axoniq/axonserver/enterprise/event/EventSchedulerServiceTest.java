package io.axoniq.axonserver.enterprise.event;

import io.axoniq.axonserver.enterprise.cluster.CompletableStreamObserver;
import io.axoniq.axonserver.enterprise.taskscheduler.TaskPublisher;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.InstructionAck;
import io.axoniq.axonserver.grpc.event.CancelScheduledEventRequest;
import io.axoniq.axonserver.grpc.event.RescheduleEventRequest;
import io.axoniq.axonserver.grpc.event.ScheduleEventRequest;
import io.axoniq.axonserver.grpc.event.ScheduleToken;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Marc Gathier
 */
public class EventSchedulerServiceTest {

    public static final String NOLEADER = "NOLEADER";
    public static final String CONTEXTWITHLEADER = "CONTEXTWITHLEADER";
    private EventSchedulerService testSubject;
    private AtomicReference<String> contextHolder = new AtomicReference<>();
    private Logger logger = LoggerFactory.getLogger(EventSchedulerServiceTest.class);


    @Before
    public void setup() {
        TaskPublisher taskPublisher = mock(TaskPublisher.class);
        CompletableFuture<?> noLeader = new CompletableFuture<>();
        noLeader.completeExceptionally(new MessagingPlatformException(
                ErrorCode.NO_LEADER_AVAILABLE, "No leader available"));

        when(taskPublisher.publishScheduledTask(anyString(), anyString(), any(), anyLong())).thenReturn(
                CompletableFuture
                        .completedFuture("task_id"));
        when(taskPublisher.publishScheduledTask(eq(NOLEADER), anyString(), any(), anyLong()))
                .thenReturn((CompletableFuture<String>) noLeader);
        when(taskPublisher.cancelScheduledTask(anyString(), anyString())).thenReturn(CompletableFuture
                                                                                             .completedFuture(null));
        when(taskPublisher.cancelScheduledTask(eq(NOLEADER), anyString()))
                .thenReturn((CompletableFuture<Void>) noLeader);

        testSubject = new EventSchedulerService(() -> contextHolder.get(), taskPublisher);
    }

    @Test
    public void scheduleEvent() throws ExecutionException, InterruptedException, TimeoutException {
        contextHolder.set(CONTEXTWITHLEADER);
        CompletableFuture<ScheduleToken> result = new CompletableFuture<>();
        testSubject.scheduleEvent(ScheduleEventRequest.newBuilder()
                                                      .build(),
                                  new CompletableStreamObserver<>(result, "scheduleEvent", logger));

        result.get(1, TimeUnit.SECONDS);
    }

    @Test
    public void scheduleEventContextWithoutLeader() throws ExecutionException, InterruptedException, TimeoutException {
        contextHolder.set(NOLEADER);
        CompletableFuture<ScheduleToken> result = new CompletableFuture<>();
        testSubject.scheduleEvent(ScheduleEventRequest.newBuilder()
                                                      .build(),
                                  new CompletableStreamObserver<>(result, "scheduleEvent", logger));

        try {
            result.get(1, TimeUnit.SECONDS);
            fail("Should fail without leader");
        } catch (ExecutionException ex) {
            assertTrue(ex.getCause() instanceof MessagingPlatformException);
            assertEquals(ErrorCode.NO_LEADER_AVAILABLE, ((MessagingPlatformException) ex.getCause()).getErrorCode());
        }
    }

    @Test
    public void rescheduleEvent() throws InterruptedException, ExecutionException, TimeoutException {
        contextHolder.set(CONTEXTWITHLEADER);
        CompletableFuture<ScheduleToken> result = new CompletableFuture<>();
        testSubject.rescheduleEvent(RescheduleEventRequest.newBuilder()
                                                          .setToken("the-old-token")
                                                          .build(),
                                    new CompletableStreamObserver<>(result, "rescheduleEvent", logger));

        result.get(1, TimeUnit.SECONDS);
    }

    @Test
    public void rescheduleEventNoToken() throws InterruptedException, ExecutionException, TimeoutException {
        contextHolder.set(CONTEXTWITHLEADER);
        CompletableFuture<ScheduleToken> result = new CompletableFuture<>();
        testSubject.rescheduleEvent(RescheduleEventRequest.newBuilder()
                                                          .build(),
                                    new CompletableStreamObserver<>(result, "rescheduleEvent", logger));

        result.get(1, TimeUnit.SECONDS);
    }

    @Test
    public void rescheduleEventWithoutLeader() throws InterruptedException, ExecutionException, TimeoutException {
        contextHolder.set(NOLEADER);
        CompletableFuture<ScheduleToken> result = new CompletableFuture<>();
        testSubject.rescheduleEvent(RescheduleEventRequest.newBuilder()
                                                          .setToken("the-old-token")
                                                          .build(),
                                    new CompletableStreamObserver<>(result, "rescheduleEvent", logger));

        try {
            result.get(1, TimeUnit.SECONDS);
            fail("Should fail without leader");
        } catch (ExecutionException ex) {
            assertTrue(ex.getCause() instanceof MessagingPlatformException);
            assertEquals(ErrorCode.NO_LEADER_AVAILABLE, ((MessagingPlatformException) ex.getCause()).getErrorCode());
        }
    }

    @Test
    public void cancelScheduledEvent() throws InterruptedException, ExecutionException, TimeoutException {
        contextHolder.set(CONTEXTWITHLEADER);
        CompletableFuture<InstructionAck> result = new CompletableFuture<>();
        testSubject.cancelScheduledEvent(CancelScheduledEventRequest.newBuilder()
                                                                    .build(),
                                         new CompletableStreamObserver<>(result, "cancelScheduledEvent", logger));

        result.get(1, TimeUnit.SECONDS);
    }
}
