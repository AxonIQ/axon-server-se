package io.axoniq.axonserver.enterprise.event;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.enterprise.cluster.internal.ContextAddingInterceptor;
import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.event.CancelScheduledEventRequest;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.event.EventSchedulerGrpc;
import io.axoniq.axonserver.grpc.event.EventStoreGrpc;
import io.axoniq.axonserver.grpc.event.EventWithToken;
import io.axoniq.axonserver.grpc.event.GetEventsRequest;
import io.axoniq.axonserver.grpc.event.GetLastTokenRequest;
import io.axoniq.axonserver.grpc.event.RescheduleEventRequest;
import io.axoniq.axonserver.grpc.event.ScheduleEventRequest;
import io.axoniq.axonserver.grpc.event.ScheduleToken;
import io.axoniq.axonserver.grpc.event.TrackingToken;
import io.axoniq.axonserver.util.AxonServerContainer;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.*;
import org.testcontainers.containers.GenericContainer;

import java.time.Duration;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class EventSchedulerServiceITCase {

    @ClassRule
    public static GenericContainer axonServer = AxonServerContainer.getInstance("axonserver-enterprise-1", "localhost");
    private EventSchedulerGrpc.EventSchedulerFutureStub stub;
    private ManagedChannel channel;

    @Before
    public void init() throws InterruptedException {
        channel = ManagedChannelBuilder.forAddress(axonServer.getContainerIpAddress(),
                                                   axonServer.getMappedPort(8124))
                                       .usePlaintext()
                                       .intercept(new ContextAddingInterceptor(() -> "default"))
                                       .build();
        stub = EventSchedulerGrpc.newFutureStub(channel);

        EventStoreGrpc.EventStoreBlockingStub eventStoreStub = EventStoreGrpc.newBlockingStub(channel);
        boolean success = false;
        while (!success) {
            try {
                eventStoreStub.getLastToken(GetLastTokenRequest.newBuilder().build());
                success = true;
            } catch (Exception ex) {
                Thread.sleep(Duration.ofSeconds(1).toMillis());
            }
        }
    }


    @Test
    public void scheduleEvent() throws InterruptedException, ExecutionException, TimeoutException {
        CountDownLatch eventReceived = new CountDownLatch(1);
        startTrackingEventProcessor(eventReceived);
        ScheduleEventRequest request = scheduledEventRequest(1500, "EventData");
        ScheduleToken response = stub.scheduleEvent(request).get(5, TimeUnit.SECONDS);
        assertNotNull(response.getToken());
        assertTrue(eventReceived.await(20, TimeUnit.SECONDS));
    }

    @Nonnull
    private ScheduleEventRequest scheduledEventRequest(long delay, String data) {
        long runAt = System.currentTimeMillis() + delay;
        return ScheduleEventRequest.newBuilder()
                                   .setInstant(runAt)
                                   .setEvent(
                                           Event.newBuilder()
                                                .setMessageIdentifier(UUID.randomUUID().toString())
                                                .setPayload(
                                                        SerializedObject.newBuilder()
                                                                        .setType("EventType")
                                                                        .setData(ByteString.copyFromUtf8(data))
                                                                        .build())
                                                .putMetaData("scheduledAt",
                                                             MetaDataValue.newBuilder()
                                                                          .setTextValue(String.valueOf(new Date()))
                                                                          .build())
                                                .putMetaData("expectedAt",
                                                             MetaDataValue.newBuilder()
                                                                          .setTextValue(String.valueOf(new Date(runAt)))
                                                                          .build())
                                                .build()).build();
    }

    private void startTrackingEventProcessor(CountDownLatch eventReceived) {
        EventStoreGrpc.EventStoreStub eventStoreStub = EventStoreGrpc.newStub(channel);
        StreamObserver<GetEventsRequest> trackerRequestStream = eventStoreStub
                .listEvents(new StreamObserver<EventWithToken>() {
                    @Override
                    public void onNext(EventWithToken eventWithToken) {
                        eventReceived.countDown();
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        throwable.printStackTrace();
                    }

                    @Override
                    public void onCompleted() {

                    }
                });
        eventStoreStub.getLastToken(GetLastTokenRequest.newBuilder().build(), new StreamObserver<TrackingToken>() {
                                        @Override
                                        public void onNext(TrackingToken trackingToken) {
                                            trackerRequestStream.onNext(GetEventsRequest.newBuilder()
                                                                                        .setTrackingToken(trackingToken.getToken() + 1)
                                                                                        .setNumberOfPermits(1000000)
                                                                                        .build());
                                        }

                                        @Override
                                        public void onError(Throwable throwable) {

                                        }

                                        @Override
                                        public void onCompleted() {

                                        }
                                    }
        );
    }

    @Test
    public void scheduleManyEvents() throws InterruptedException {
        int count = 1000;
        CountDownLatch eventReceived = new CountDownLatch(count);
        startTrackingEventProcessor(eventReceived);

        IntStream.range(0, count).forEach(i -> {
            stub.scheduleEvent(scheduledEventRequest((int) (Math.random()
                    * TimeUnit.SECONDS.toMillis(20)), "EventData" + i));
        });
        assertTrue(eventReceived.await(60, TimeUnit.SECONDS));
    }


    @Test
    public void rescheduleEvent() throws InterruptedException, ExecutionException, TimeoutException {
        ScheduleEventRequest request = scheduledEventRequest(Duration.ofSeconds(45)
                                                                     .toMillis(), "EventData");
        ScheduleToken response = stub.scheduleEvent(request).get(5, TimeUnit.SECONDS);
        Thread.sleep(1000);
        RescheduleEventRequest reschedule = RescheduleEventRequest.newBuilder()
                                                                  .setInstant(System.currentTimeMillis() + Duration
                                                                          .ofSeconds(5).toMillis())
                                                                  .setToken(response.getToken())
                                                                  .build();
        stub.rescheduleEvent(reschedule).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void cancelScheduledEvent() throws InterruptedException, ExecutionException, TimeoutException {
        ScheduleEventRequest request = scheduledEventRequest(Duration.ofSeconds(45)
                                                                     .toMillis(), "EventData");
        ScheduleToken response = stub.scheduleEvent(request).get(5, TimeUnit.SECONDS);
        Thread.sleep(1000);
        CancelScheduledEventRequest cancelRequest = CancelScheduledEventRequest.newBuilder()
                                                                               .setToken(response.getToken())
                                                                               .build();
        stub.cancelScheduledEvent(cancelRequest).get(5, TimeUnit.SECONDS);
    }

    @Test
    public void cancelScheduledEventUnknownToken() throws InterruptedException, ExecutionException, TimeoutException {
        CancelScheduledEventRequest cancelRequest = CancelScheduledEventRequest.newBuilder()
                                                                               .setToken("Unknown")
                                                                               .build();
        stub.cancelScheduledEvent(cancelRequest).get(5, TimeUnit.SECONDS);
    }
}