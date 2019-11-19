package io.axoniq.axonserver.grpc.heartbeat;

import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.applicationevents.TopologyEvents.ApplicationConnected;
import io.axoniq.axonserver.applicationevents.TopologyEvents.ApplicationDisconnected;
import io.axoniq.axonserver.grpc.control.Heartbeat;
import io.axoniq.axonserver.grpc.control.PlatformInboundInstruction;
import io.axoniq.axonserver.message.ClientIdentification;
import io.axoniq.axonserver.util.FakeClock;
import org.junit.*;

import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static io.axoniq.axonserver.grpc.control.PlatformInboundInstruction.newBuilder;
import static java.time.temporal.ChronoUnit.MILLIS;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link HeartbeatMonitor}
 *
 * @author Sara Pellegrini
 */
public class HeartbeatMonitorTest {

    private final ClientIdentification client4_2_1 = new ClientIdentification("A", "A");
    private final ApplicationConnected client4_2_1Connected =
            new ApplicationConnected("A", "A", "A", "4.2.1");
    private final ApplicationDisconnected client4_2_1Disconnected =
            new ApplicationDisconnected("A", "A", "A");

    private final ClientIdentification client4_2 = new ClientIdentification("B", "B");
    private final ApplicationConnected client4_2Connected =
            new ApplicationConnected("B", "B", "B", "4.2");
    private final ApplicationDisconnected client4_2Disconnected =
            new ApplicationDisconnected("B", "B", "B");

    private final PlatformInboundInstruction heartbeat = newBuilder().setHeartbeat(Heartbeat.newBuilder()).build();

    @Test
    public void testConnectionActive() {
        AtomicReference<Instant> instant = new AtomicReference<>(Instant.now());
        AtomicReference<BiConsumer<ClientIdentification, PlatformInboundInstruction>> listener = new AtomicReference<>();
        List<Object> publishedEvents = new LinkedList<>();
        HeartbeatMonitor testSubject = new HeartbeatMonitor(listener::set,
                                                            publishedEvents::add,
                                                            hb -> listener.get().accept(client4_2_1, heartbeat),
                                                            5000,
                                                            new FakeClock(instant::get));
        testSubject.on(client4_2_1Connected);
        testSubject.sendHeartbeat();
        instant.set(instant.get().plus(3000, MILLIS));
        testSubject.checkClientsStillAlive();
        testSubject.on(client4_2_1Disconnected);
        assertTrue(publishedEvents.isEmpty());
    }

    @Test
    public void testConnectionNotActive() {
        AtomicReference<Instant> instant = new AtomicReference<>(Instant.now());
        AtomicReference<BiConsumer<ClientIdentification, PlatformInboundInstruction>> listener = new AtomicReference<>();
        List<Object> publishedEvents = new LinkedList<>();
        HeartbeatMonitor testSubject = new HeartbeatMonitor(listener::set,
                                                            publishedEvents::add,
                                                            hb -> listener.get().accept(client4_2_1, heartbeat),
                                                            5000,
                                                            new FakeClock(instant::get));
        testSubject.on(client4_2_1Connected);
        testSubject.sendHeartbeat();
        instant.set(instant.get().plus(6000, MILLIS));
        testSubject.checkClientsStillAlive();
        testSubject.on(client4_2_1Disconnected);
        assertFalse(publishedEvents.isEmpty());
        assertTrue(publishedEvents.get(0) instanceof TopologyEvents.ApplicationInactivityTimeout);
    }

    @Test
    public void testHeartbeatNotSupportedByClient() {
        AtomicReference<Instant> instant = new AtomicReference<>(Instant.now());
        AtomicReference<BiConsumer<ClientIdentification, PlatformInboundInstruction>> listener = new AtomicReference<>();

        List<Object> publishedEvents = new LinkedList<>();
        HeartbeatMonitor testSubject = new HeartbeatMonitor(listener::set,
                                                            publishedEvents::add,
                                                            hb -> {
                                                            },
                                                            5000,
                                                            new FakeClock(instant::get));
        testSubject.on(client4_2_1Connected);
        testSubject.sendHeartbeat();
        instant.set(instant.get().plus(6000, MILLIS));
        testSubject.checkClientsStillAlive();
        testSubject.on(client4_2_1Disconnected);
        assertTrue(publishedEvents.isEmpty());
    }
}
