package io.axoniq.axonserver.grpc.heartbeat;

import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.applicationevents.TopologyEvents.ApplicationConnected;
import io.axoniq.axonserver.applicationevents.TopologyEvents.ApplicationDisconnected;
import io.axoniq.axonserver.grpc.control.Heartbeat;
import io.axoniq.axonserver.grpc.control.PlatformInboundInstruction;
import io.axoniq.axonserver.message.ClientStreamIdentification;
import io.axoniq.axonserver.test.FakeClock;
import org.junit.*;

import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static io.axoniq.axonserver.grpc.control.PlatformInboundInstruction.newBuilder;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link HeartbeatMonitor}
 *
 * @author Sara Pellegrini
 */
public class HeartbeatMonitorTest {

    private final ClientStreamIdentification client4_2_1 = new ClientStreamIdentification("A", "A");
    private final ApplicationConnected client4_2_1Connected =
            new ApplicationConnected("A", "A", "A", "A", "4.2.1");
    private final ApplicationDisconnected client4_2_1Disconnected =
            new ApplicationDisconnected("A", "A", "A", "test");

    private final ClientStreamIdentification client4_2 = new ClientStreamIdentification("B", "B");
    private final ApplicationConnected client4_2Connected =
            new ApplicationConnected("B", "B", "B", "B", "4.2");
    private final ApplicationDisconnected client4_2Disconnected =
            new ApplicationDisconnected("B", "B", "B", "test");

    private final PlatformInboundInstruction heartbeat = newBuilder().setHeartbeat(Heartbeat.newBuilder()).build();

    @Test
    public void testConnectionActive() {
        FakeClock clock = new FakeClock(Instant.now());
        AtomicReference<BiConsumer<ClientStreamIdentification, PlatformInboundInstruction>> listener = new AtomicReference<>();
        List<Object> publishedEvents = new LinkedList<>();
        HeartbeatMonitor testSubject = new HeartbeatMonitor(listener::set,
                                                            publishedEvents::add,
                                                            hb -> listener.get().accept(client4_2_1, heartbeat),
                                                            5000,
                                                            clock);
        testSubject.on(client4_2_1Connected);
        testSubject.sendHeartbeat();
        clock.timeElapses(3000, TimeUnit.MILLISECONDS);
        testSubject.checkClientsStillAlive();
        testSubject.on(client4_2_1Disconnected);
        assertTrue(publishedEvents.isEmpty());
    }

    @Test
    public void testConnectionNotActive() {
        FakeClock clock = new FakeClock(Instant.now());
        AtomicReference<BiConsumer<ClientStreamIdentification, PlatformInboundInstruction>> listener = new AtomicReference<>();
        List<Object> publishedEvents = new LinkedList<>();
        HeartbeatMonitor testSubject = new HeartbeatMonitor(listener::set,
                                                            publishedEvents::add,
                                                            hb -> listener.get().accept(client4_2_1, heartbeat),
                                                            5000,
                                                            clock);
        testSubject.on(client4_2_1Connected);
        testSubject.sendHeartbeat();
        clock.timeElapses(6000, TimeUnit.MILLISECONDS);
        testSubject.checkClientsStillAlive();
        testSubject.on(client4_2_1Disconnected);
        assertFalse(publishedEvents.isEmpty());
        assertTrue(publishedEvents.get(0) instanceof TopologyEvents.ApplicationInactivityTimeout);
    }

    @Test
    public void testHeartbeatNotSupportedByClient() {
        FakeClock clock = new FakeClock(Instant.now());
        AtomicReference<BiConsumer<ClientStreamIdentification, PlatformInboundInstruction>> listener = new AtomicReference<>();

        List<Object> publishedEvents = new LinkedList<>();
        HeartbeatMonitor testSubject = new HeartbeatMonitor(listener::set,
                                                            publishedEvents::add,
                                                            hb -> {
                                                            },
                                                            5000,
                                                            clock);
        testSubject.on(client4_2_1Connected);
        testSubject.sendHeartbeat();
        clock.timeElapses(6000, TimeUnit.MILLISECONDS);
        testSubject.checkClientsStillAlive();
        testSubject.on(client4_2_1Disconnected);
        assertTrue(publishedEvents.isEmpty());
    }
}
