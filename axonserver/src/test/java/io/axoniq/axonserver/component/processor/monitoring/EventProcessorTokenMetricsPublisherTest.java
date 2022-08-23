package io.axoniq.axonserver.component.processor.monitoring;

import com.google.common.util.concurrent.AtomicDouble;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import io.axoniq.axonserver.message.event.EventDispatcher;
import io.axoniq.axonserver.metric.BaseMetricName;
import io.axoniq.axonserver.metric.MeterFactory;
import io.micrometer.core.instrument.Gauge;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class EventProcessorTokenMetricsPublisherTest {
    private final MeterFactory meterFactory = mock(MeterFactory.class);
    private final EventDispatcher eventDispatcher = mock(EventDispatcher.class);
    private final EventProcessorTokenMetricsPublisher publisher = new EventProcessorTokenMetricsPublisher(eventDispatcher, meterFactory);
    private final AtomicReference<AtomicDouble> lowestPositionValue = new AtomicReference<>();
    private final AtomicReference<AtomicDouble> eventLagValue = new AtomicReference<>();

    final EventProcessorKey key = mock(EventProcessorKey.class);

    @BeforeEach
    void setUp() {
        when(meterFactory.gauge(eq(BaseMetricName.AXON_EVENT_PROCESSOR_EVENT_POSITION), any(), any(), any())).thenAnswer(invocationOnMock -> {
            final AtomicDouble argument = invocationOnMock.getArgument(2, AtomicDouble.class);
            lowestPositionValue.set(argument);
            return Mockito.mock(Gauge.class);
        });
        when(meterFactory.gauge(eq(BaseMetricName.AXON_EVENT_PROCESSOR_EVENT_LAG), any(), any(), any())).thenAnswer(invocationOnMock -> {
            final AtomicDouble argument = invocationOnMock.getArgument(2, AtomicDouble.class);
            eventLagValue.set(argument);
            return Mockito.mock(Gauge.class);
        });
        when(key.getContext()).thenReturn("myContext");
        when(eventDispatcher.getNrOfEvents("myContext")).thenReturn(1000L);
    }

    @Test
    void determinesLowestTokenPositionAndLag() {
        publisher.publishFor(key, Arrays.asList(
                EventProcessorInfo.newBuilder().addSegmentStatus(
                        EventProcessorInfo.SegmentStatus.newBuilder().setTokenPosition(900).build()
                ).build(),
                EventProcessorInfo.newBuilder().addSegmentStatus(
                        EventProcessorInfo.SegmentStatus.newBuilder().setTokenPosition(800).build()
                ).build(),
                EventProcessorInfo.newBuilder().addSegmentStatus(
                        EventProcessorInfo.SegmentStatus.newBuilder().setTokenPosition(950).build()
                ).build(),
                EventProcessorInfo.newBuilder().addSegmentStatus(
                        EventProcessorInfo.SegmentStatus.newBuilder().setTokenPosition(970).build()
                ).build(),
                EventProcessorInfo.newBuilder().addSegmentStatus(
                        EventProcessorInfo.SegmentStatus.newBuilder().setTokenPosition(890).build()
                ).build()
        ));

        assertEquals(800d, lowestPositionValue.get().get());
        assertEquals(200d, eventLagValue.get().get());
    }

    @Test
    void determinesZeroWhenNoSegments() {
        publisher.publishFor(key, emptyList());

        assertEquals(0d, lowestPositionValue.get().get());
        assertEquals(1000d, eventLagValue.get().get());
    }
}
