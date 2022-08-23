package io.axoniq.axonserver.component.processor.monitoring;

import com.google.common.util.concurrent.AtomicDouble;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import io.axoniq.axonserver.metric.BaseMetricName;
import io.axoniq.axonserver.metric.MeterFactory;
import io.micrometer.core.instrument.Gauge;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class EventProcessorSegmentMetricsPublisherTest {
    private final MeterFactory meterFactory = mock(MeterFactory.class);
    private final EventProcessorSegmentMetricsPublisher publisher = new EventProcessorSegmentMetricsPublisher(meterFactory);
    private final AtomicReference<AtomicDouble> segmentMetricValue = new AtomicReference<>();

    @BeforeEach
    void setUp() {
        when(meterFactory.gauge(eq(BaseMetricName.AXON_EVENT_PROCESSOR_SEGMENTS_UNCLAIMED), any(), any(), any())).thenAnswer(invocationOnMock -> {
            final AtomicDouble argument = invocationOnMock.getArgument(2, AtomicDouble.class);
            segmentMetricValue.set(argument);
            return Mockito.mock(Gauge.class);
        });
    }

    @Test
    void unclaimedSegmentsIsOneWhenThereAreNoSegments() {
        publisher.publishFor(mock(EventProcessorKey.class), Collections.emptyList());

        assertNotNull(segmentMetricValue.get());
        assertEquals(1d, segmentMetricValue.get().get());
    }

    @Test
    void unclaimedSegmentsIsZeroWhenSingleIsClaimed() {
        publisher.publishFor(mock(EventProcessorKey.class), singletonList(EventProcessorInfo.newBuilder()
                .addSegmentStatus(EventProcessorInfo.SegmentStatus.newBuilder().setSegmentId(0).setOnePartOf(1).build())
                .build()));

        assertNotNull(segmentMetricValue.get());
        assertEquals(0d, segmentMetricValue.get().get());
    }

    @Test
    void unclaimedSegmentsIsQuarterWhenSomeAreClaimed() {
        publisher.publishFor(mock(EventProcessorKey.class), singletonList(EventProcessorInfo.newBuilder()
                .addSegmentStatus(EventProcessorInfo.SegmentStatus.newBuilder().setSegmentId(1).setOnePartOf(4).build())
                .addSegmentStatus(EventProcessorInfo.SegmentStatus.newBuilder().setSegmentId(2).setOnePartOf(2).build())
                .build()));

        assertNotNull(segmentMetricValue.get());
        assertEquals(0.25d, segmentMetricValue.get().get());
    }

    @Test
    void unclaimedSegmentsDoesNotCountSegmentsWithSameIdTwice() {
        publisher.publishFor(mock(EventProcessorKey.class), singletonList(EventProcessorInfo.newBuilder()
                .addSegmentStatus(EventProcessorInfo.SegmentStatus.newBuilder().setSegmentId(1).setOnePartOf(4).build())
                .addSegmentStatus(EventProcessorInfo.SegmentStatus.newBuilder().setSegmentId(1).setOnePartOf(4).build())
                .addSegmentStatus(EventProcessorInfo.SegmentStatus.newBuilder().setSegmentId(2).setOnePartOf(2).build())
                .build()));

        assertNotNull(segmentMetricValue.get());
        assertEquals(0.25d, segmentMetricValue.get().get());
    }

    @Test
    void unclaimedSegmentsExcludesSegmentsInErrorState() {
        publisher.publishFor(mock(EventProcessorKey.class), singletonList(EventProcessorInfo.newBuilder()
                .addSegmentStatus(EventProcessorInfo.SegmentStatus.newBuilder().setSegmentId(1).setOnePartOf(4).build())
                .addSegmentStatus(EventProcessorInfo.SegmentStatus.newBuilder().setSegmentId(2).setOnePartOf(2).setErrorState("MyException: Blabla").build())
                .build()));

        assertNotNull(segmentMetricValue.get());
        assertEquals(0.75d, segmentMetricValue.get().get());
    }
}
