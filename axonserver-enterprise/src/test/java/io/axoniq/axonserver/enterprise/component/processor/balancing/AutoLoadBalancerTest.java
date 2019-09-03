package io.axoniq.axonserver.enterprise.component.processor.balancing;

import io.axoniq.axonserver.applicationevents.EventProcessorEvents;
import io.axoniq.axonserver.applicationevents.TopologyEvents;
import io.axoniq.axonserver.component.processor.ClientEventProcessorInfo;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import org.junit.*;

import static org.junit.Assert.*;

/**
 * @author Marc Gathier
 */
public class AutoLoadBalancerTest {

    private int balanceCounter;
    private boolean leader = true;
    private AutoLoadBalancer testSubject = new AutoLoadBalancer(t -> balanceCounter++, c -> leader);

    @Test
    public void onEventProcessorStatusChange() {
        testSubject.onEventProcessorStatusChange(new EventProcessorEvents.EventProcessorStatusUpdated(
                new ClientEventProcessorInfo("testClient", "testContext", EventProcessorInfo.newBuilder()
                                                                                            .setProcessorName(
                                                                                                    "sampleProcessor")
                                                                                            .setActiveThreads(1)
                                                                                            .addSegmentStatus(
                                                                                                    EventProcessorInfo.SegmentStatus
                                                                                                            .newBuilder()
                                                                                                            .setSegmentId(
                                                                                                                    1)
                                                                                                            .setTokenPosition(
                                                                                                                    1000)
                                                                                                            .build())
                                                                                            .build()), false));
        assertEquals(1, balanceCounter);
        testSubject.onEventProcessorStatusChange(new EventProcessorEvents.EventProcessorStatusUpdated(
                new ClientEventProcessorInfo("testClient", "testContext", EventProcessorInfo.newBuilder()
                                                                                            .setProcessorName(
                                                                                                    "sampleProcessor")
                                                                                            .setActiveThreads(1)
                                                                                            .addSegmentStatus(
                                                                                                    EventProcessorInfo.SegmentStatus
                                                                                                            .newBuilder()
                                                                                                            .setSegmentId(
                                                                                                                    1)
                                                                                                            .setTokenPosition(
                                                                                                                    1001)
                                                                                                            .build())
                                                                                            .build()), false));
        assertEquals(1, balanceCounter);
        testSubject.onEventProcessorStatusChange(new EventProcessorEvents.EventProcessorStatusUpdated(
                new ClientEventProcessorInfo("testClient", "testContext", EventProcessorInfo.newBuilder()
                                                                                            .setProcessorName(
                                                                                                    "sampleProcessor")
                                                                                            .setActiveThreads(0)
                                                                                            .build()), false));
        assertEquals(2, balanceCounter);
    }

    @Test
    public void onClientDisconnected() {
        testSubject.onEventProcessorStatusChange(new EventProcessorEvents.EventProcessorStatusUpdated(
                new ClientEventProcessorInfo("testClient", "testContext", EventProcessorInfo.newBuilder()
                                                                                            .setProcessorName(
                                                                                                    "sampleProcessor")
                                                                                            .setActiveThreads(1)
                                                                                            .addSegmentStatus(
                                                                                                    EventProcessorInfo.SegmentStatus
                                                                                                            .newBuilder()
                                                                                                            .setSegmentId(
                                                                                                                    1)
                                                                                                            .setTokenPosition(
                                                                                                                    1000)
                                                                                                            .build())
                                                                                            .build()), false));
        assertEquals(1, balanceCounter);
        testSubject.onClientDisconnected(new TopologyEvents.ApplicationDisconnected("testContext",
                                                                                    "testComponent",
                                                                                    "testClient"));
        assertEquals(2, balanceCounter);
        testSubject.onClientDisconnected(new TopologyEvents.ApplicationDisconnected("testContext",
                                                                                    "testComponent",
                                                                                    "testClient"));
        assertEquals(2, balanceCounter);
    }
}