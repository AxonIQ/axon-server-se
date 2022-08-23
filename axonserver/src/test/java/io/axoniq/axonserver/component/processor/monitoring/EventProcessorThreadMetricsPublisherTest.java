package io.axoniq.axonserver.component.processor.monitoring;

import com.google.common.util.concurrent.AtomicDouble;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import io.axoniq.axonserver.metric.BaseMetricName;
import io.axoniq.axonserver.metric.MeterFactory;
import io.micrometer.core.instrument.Gauge;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class EventProcessorThreadMetricsPublisherTest {
    private final MeterFactory meterFactory = mock(MeterFactory.class);
    private final EventProcessorThreadMetricsPublisher publisher = new EventProcessorThreadMetricsPublisher(meterFactory);
    private final AtomicReference<AtomicDouble> activeMetricValue = new AtomicReference<>();
    private final AtomicReference<AtomicDouble> availableMetricValue = new AtomicReference<>();

    @BeforeEach
    void setUp() {
        when(meterFactory.gauge(eq(BaseMetricName.AXON_EVENT_PROCESSOR_THREADS_ACTIVE), any(), any(), any())).thenAnswer(invocationOnMock -> {
            final AtomicDouble argument = invocationOnMock.getArgument(2, AtomicDouble.class);
            activeMetricValue.set(argument);
            return Mockito.mock(Gauge.class);
        });
        when(meterFactory.gauge(eq(BaseMetricName.AXON_EVENT_PROCESSOR_THREADS_AVAILABLE), any(), any(), any())).thenAnswer(invocationOnMock -> {
            final AtomicDouble argument = invocationOnMock.getArgument(2, AtomicDouble.class);
            availableMetricValue.set(argument);
            return Mockito.mock(Gauge.class);
        });
    }

    @Test
    void addsActiveAndAvailableThreads() {
        publisher.publishFor(mock(EventProcessorKey.class), Arrays.asList(
                EventProcessorInfo.newBuilder().setActiveThreads(1).setAvailableThreads(2).build(),
                EventProcessorInfo.newBuilder().setActiveThreads(1).setAvailableThreads(2).build(),
                EventProcessorInfo.newBuilder().setActiveThreads(1).setAvailableThreads(2).build(),
                EventProcessorInfo.newBuilder().setActiveThreads(1).setAvailableThreads(2).build(),
                EventProcessorInfo.newBuilder().setActiveThreads(1).setAvailableThreads(2).build()
        ));

        assertEquals(5d, activeMetricValue.get().get());
        assertEquals(10d, availableMetricValue.get().get());
    }

    @Test
    void skipsAvailableMetricsWhenPooledStreaming() {
        publisher.publishFor(mock(EventProcessorKey.class), Arrays.asList(
                EventProcessorInfo.newBuilder().setMode("Pooled Streaming").setActiveThreads(1).setAvailableThreads(2).build(),
                EventProcessorInfo.newBuilder().setMode("Pooled Streaming").setActiveThreads(1).setAvailableThreads(2).build(),
                EventProcessorInfo.newBuilder().setMode("Pooled Streaming").setActiveThreads(1).setAvailableThreads(2).build(),
                EventProcessorInfo.newBuilder().setMode("Pooled Streaming").setActiveThreads(1).setAvailableThreads(2).build(),
                EventProcessorInfo.newBuilder().setMode("Pooled Streaming").setActiveThreads(1).setAvailableThreads(2).build()
        ));

        assertNull(availableMetricValue.get());
    }
}
