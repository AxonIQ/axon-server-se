package io.axoniq.axonserver.enterprise.cluster.internal;

import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.TestSystemInfoProvider;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.SafepointRepository;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.context.ContextController;
import io.axoniq.axonserver.enterprise.jpa.Context;
import io.axoniq.axonserver.grpc.DataSychronizationServiceInterface;
import io.axoniq.axonserver.internal.grpc.SafepointMessage;
import io.axoniq.axonserver.internal.grpc.SynchronizationReplicaInbound;
import io.axoniq.axonserver.internal.grpc.SynchronizationReplicaOutbound;
import io.axoniq.axonserver.internal.grpc.TransactionWithToken;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import io.axoniq.axonserver.topology.Topology;
import io.grpc.stub.StreamObserver;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.runners.*;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Author: marc
 */
@RunWith(MockitoJUnitRunner.class)
public class DataSynchronizationReplicaTest {
    private DataSynchronizationReplica testSubject;

    @Mock
    private ClusterController clusterController;
    @Mock
    private LocalEventStore localEventStore;
    @Mock
    private SafepointRepository safepointRespository;
    private FakeClock clock = new FakeClock();


    Collection<SynchronizationReplicaOutbound> sentMessages = new CopyOnWriteArrayList<>();
    AtomicBoolean completed = new AtomicBoolean();
    AtomicReference<Throwable> exception = new AtomicReference<>();
    AtomicReference<StreamObserver<SynchronizationReplicaInbound>> inboundStream = new AtomicReference<>();

    @Before
    public void setUp()  {
        when(clusterController.getName()).thenReturn("me");
        ClusterNode myNode = new ClusterNode("me", "host", "host", 0, 0, 0);
        myNode.addContext(new Context(Topology.DEFAULT_CONTEXT), true, true);
        when(clusterController.getMe()).thenReturn(myNode);
        when(safepointRespository.findById(any())).thenReturn(Optional.empty());
        doAnswer(invocationOnMock -> {
            TransactionWithToken t = (TransactionWithToken)invocationOnMock.getArguments()[1];

            return t.getToken() + t.getEventsCount();
        }).when(localEventStore).syncEvents(any(), any());
        doAnswer(invocationOnMock -> {
            TransactionWithToken t = (TransactionWithToken)invocationOnMock.getArguments()[1];

            return t.getToken() + t.getEventsCount();
        }).when(localEventStore).syncSnapshots(any(), any());
        ApplicationEventPublisher applicationEventPublisher = new ApplicationEventPublisher() {
            @Override
            public void publishEvent(ApplicationEvent applicationEvent) {

            }

            @Override
            public void publishEvent(Object o) {
                if (o instanceof ClusterEvents.MasterDisconnected) {
                    testSubject.on((ClusterEvents.MasterDisconnected) o);
                }
            }
        };

        MessagingPlatformConfiguration messagingPlatformConfiguration = new MessagingPlatformConfiguration(new TestSystemInfoProvider());
        messagingPlatformConfiguration.getCommandFlowControl().setThreshold(10);
        messagingPlatformConfiguration.getCommandFlowControl().setInitialPermits(20);
        messagingPlatformConfiguration.getCommandFlowControl().setNewPermits(20);
        StubFactory stubFactory = new StubFactory() {
            @Override
            public MessagingClusterServiceInterface messagingClusterServiceStub(
                    MessagingPlatformConfiguration messagingPlatformConfiguration, ClusterNode clusterNode) {
                return null;
            }

            @Override
            public MessagingClusterServiceInterface messagingClusterServiceStub(
                    MessagingPlatformConfiguration messagingPlatformConfiguration, String host, int port) {
                return null;
            }

            @Override
            public DataSychronizationServiceInterface dataSynchronizationServiceStub(
                    MessagingPlatformConfiguration messagingPlatformConfiguration, ClusterNode clusterNode) {
                return responseObserver -> {
                    inboundStream.set(responseObserver);
                    return new StreamObserver<SynchronizationReplicaOutbound>() {
                        @Override
                        public void onNext(SynchronizationReplicaOutbound o) {
                            sentMessages.add(o);
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            exception.set(throwable);
                        }

                        @Override
                        public void onCompleted() {
                            completed.set(true);
                        }
                    };
                };
            }
        };
        testSubject = new DataSynchronizationReplica(clusterController, messagingPlatformConfiguration, stubFactory, localEventStore,
                                                     applicationEventPublisher, safepointRespository, clock);
    }

    @Test
    public void masterConfirmation() {
        testSubject.on(new ClusterEvents.MasterConfirmation(Topology.DEFAULT_CONTEXT, "demo", false));
        assertEquals(1, sentMessages.size());
        assertTrue(testSubject.getConnectionPerContext().containsKey(Topology.DEFAULT_CONTEXT));
    }

    @Test
    public void stepDown() {
        testSubject.on(new ClusterEvents.MasterConfirmation(Topology.DEFAULT_CONTEXT, "demo", false));

        testSubject.on(new ClusterEvents.MasterStepDown(Topology.DEFAULT_CONTEXT, false));
        assertTrue(completed.get());
        assertFalse(testSubject.getConnectionPerContext().containsKey(Topology.DEFAULT_CONTEXT));
    }

    @Test
    public void masterDisconnected() {
        testSubject.on(new ClusterEvents.MasterConfirmation(Topology.DEFAULT_CONTEXT, "demo", false));
        testSubject.on(new ClusterEvents.MasterDisconnected(Topology.DEFAULT_CONTEXT, false));
        assertTrue(completed.get());
        assertFalse(testSubject.getConnectionPerContext().containsKey(Topology.DEFAULT_CONTEXT));
    }

    @Test
    public void masterError() {
        testSubject.on(new ClusterEvents.MasterConfirmation(Topology.DEFAULT_CONTEXT, "demo", false));
        inboundStream.get().onError(new RuntimeException("Failed by JUnit"));
        assertFalse(testSubject.getConnectionPerContext().containsKey(Topology.DEFAULT_CONTEXT));
    }

    @Test
    public void masterCompleted() {
        testSubject.on(new ClusterEvents.MasterConfirmation(Topology.DEFAULT_CONTEXT, "demo", false));
        inboundStream.get().onCompleted();
        assertFalse(testSubject.getConnectionPerContext().containsKey(Topology.DEFAULT_CONTEXT));
        assertTrue(completed.get());
    }

    @Test
    public void noMessagesForOneMinute() {
        testSubject.on(new ClusterEvents.MasterConfirmation(Topology.DEFAULT_CONTEXT, "demo", false));
        clock.add(1, TimeUnit.MINUTES);
        testSubject.checkAlive();
        assertFalse(testSubject.getConnectionPerContext().containsKey(Topology.DEFAULT_CONTEXT));
        assertTrue(completed.get());
    }

    @Test
    public void receivedMessages() {
        testSubject.on(new ClusterEvents.MasterConfirmation(Topology.DEFAULT_CONTEXT, "demo", false));

        inboundStream.get().onNext(SynchronizationReplicaInbound.newBuilder()
                                                                .setSafepoint(SafepointMessage.newBuilder()
                                                                                              .setType("EVENT")
                                                                                              .setToken(100)
                                                                                              .build())
                                                                .build());
        clock.add(5, TimeUnit.SECONDS);
        testSubject.checkAlive();
        assertTrue(testSubject.getConnectionPerContext().containsKey(Topology.DEFAULT_CONTEXT));
        assertFalse(completed.get());
    }

    @Test
    public void noMessagesWhileProcessingBacklog() {
        testSubject.on(new ClusterEvents.MasterConfirmation(Topology.DEFAULT_CONTEXT, "demo", false));
        inboundStream.get().onNext(SynchronizationReplicaInbound.newBuilder()
                                                                .setSafepoint(SafepointMessage.newBuilder()
                                                                                              .setType("EVENT")
                                                                                              .setToken(100)
                                                                                              .build())
                                                                .build());
        clock.add(15, TimeUnit.SECONDS);
        testSubject.checkAlive();
        assertFalse(testSubject.getConnectionPerContext().containsKey(Topology.DEFAULT_CONTEXT));
        assertTrue(completed.get());
    }

    @Test
    public void handleInOrderEvents() {
        testSubject.on(new ClusterEvents.MasterConfirmation(Topology.DEFAULT_CONTEXT, "demo", false));
        inboundStream.get().onNext(SynchronizationReplicaInbound.newBuilder()
                                                                .setEvent(TransactionWithToken.newBuilder()
                                                                                              .setToken(1)
                                                                                              .addEvents(Event.newBuilder().build())
                                                                                              .build())
                                                                .build());
        assertEquals(2, sentMessages.size());
        inboundStream.get().onNext(SynchronizationReplicaInbound.newBuilder()
                                                                .setEvent(TransactionWithToken.newBuilder()
                                                                                              .setToken(2)
                                                                                              .addEvents(Event.newBuilder().build())
                                                                                              .build())
                                                                .build());
        assertEquals(3, sentMessages.size());
        DataSynchronizationReplica.ReplicaConnection connection = testSubject.getConnectionPerContext().get(
                Topology.DEFAULT_CONTEXT);
        assertNotNull(connection);
        assertEquals(3, connection.getExpectedEventToken());
        assertEquals(0, connection.waitingEvents());
    }

    @Test
    public void handleOutOfOrderEvents() {
        testSubject.on(new ClusterEvents.MasterConfirmation(Topology.DEFAULT_CONTEXT, "demo", false));
        inboundStream.get().onNext(SynchronizationReplicaInbound.newBuilder()
                                                                .setSnapshot(TransactionWithToken.newBuilder()
                                                                                              .setToken(2)
                                                                                              .addEvents(Event.newBuilder().build())
                                                                                              .build())
                                                                .build());
        assertEquals(1, sentMessages.size());
        DataSynchronizationReplica.ReplicaConnection connection = testSubject.getConnectionPerContext().get(
                Topology.DEFAULT_CONTEXT);
        assertNotNull(connection);
        assertEquals(1, connection.getExpectedSnapshotToken());
        assertEquals(1, connection.waitingSnapshots());
        inboundStream.get().onNext(SynchronizationReplicaInbound.newBuilder()
                                                                .setSnapshot(TransactionWithToken.newBuilder()
                                                                                              .setToken(1)
                                                                                              .addEvents(Event.newBuilder().build())
                                                                                              .build())
                                                                .build());
        assertEquals(3, sentMessages.size());

        connection = testSubject.getConnectionPerContext().get(
                Topology.DEFAULT_CONTEXT);
        assertNotNull(connection);
        assertEquals(3, connection.getExpectedSnapshotToken());
        assertEquals(0, connection.waitingSnapshots());
    }


    @Test
    public void setPermits() {
        testSubject.on(new ClusterEvents.MasterConfirmation(Topology.DEFAULT_CONTEXT, "demo", false));
        IntStream.range(0,10).forEach(i ->
        inboundStream.get().onNext(SynchronizationReplicaInbound.newBuilder()
                                                                .setSnapshot(TransactionWithToken.newBuilder()
                                                                                                 .setToken(-1)
                                                                                                 .addEvents(Event.newBuilder().build())
                                                                                                 .build())
                                                                .build()));
        assertEquals(2, sentMessages.size());

    }
}