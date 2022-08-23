package io.axoniq.axonserver.component.processor.monitoring;

import io.axoniq.axonserver.applicationevents.EventProcessorEvents;
import io.axoniq.axonserver.component.processor.ClientEventProcessorInfo;
import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.component.processor.listener.ClientProcessors;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
class EventProcessorMonitoringProviderTest {
    private final EventProcessorMetricPublisher publisher = Mockito.mock(EventProcessorMetricPublisher.class);
    private final Clock clock = Mockito.mock(Clock.class);

    private EventProcessorMonitoringProvider provider;

    @BeforeEach
    void setUp() {
        ClientProcessors clientProcessors = () -> generateProcessors().iterator();
        provider = new EventProcessorMonitoringProvider(clock, clientProcessors, Collections.singletonList(publisher));
        setTime(Instant.EPOCH);
    }

    private void setTime(Instant instant) {
        when(clock.instant()).thenReturn(instant);
    }

    @Test
    void callsPublishersWithCorrectItemsOnUpdate() {
        final ArgumentCaptor<List<EventProcessorInfo>> argumentCaptor = ArgumentCaptor.forClass((Class) List.class);

        EventProcessorKey key = new EventProcessorKey("contextOne", "componentTwo", "processorThree");
        provider.on(createEvent("contextOne", "componentTwo", "processorThree"));
        Mockito.verify(publisher).publishFor(eq(key), argumentCaptor.capture());

        List<EventProcessorInfo> value = argumentCaptor.getValue();
        assertTrue(value.stream().allMatch(EventProcessorInfo::getRunning));
        assertTrue(value.stream().allMatch(i -> i.getProcessorName().equals("processorThree")));
        assertEquals(1, value.size());
    }

    @Test
    void cleansUpAfterAnHour() {
        provider.on(createEvent("contextOne", "componentTwo", "processorThree"));

        provider.cleanup();

        verify(publisher, never()).cleanup(any());

        setTime(Instant.EPOCH.plus(1, ChronoUnit.HOURS).plus(1, ChronoUnit.SECONDS));
        provider.cleanup();
        verify(publisher).cleanup(any());
    }

    private ClientProcessor createProcessor(String context, String component, String processorName, boolean running) {
        final ClientProcessor mock = Mockito.mock(ClientProcessor.class);
        final EventProcessorInfo info = EventProcessorInfo.newBuilder()
                .setRunning(running)
                .setProcessorName(processorName)
                .build();
        when(mock.eventProcessorInfo()).thenReturn(info);
        when(mock.context()).thenReturn(context);
        when(mock.component()).thenReturn(component);

        return mock;
    }

    private EventProcessorEvents.EventProcessorStatusUpdated createEvent(String context, String component, String processorName) {
        final EventProcessorInfo info = EventProcessorInfo.newBuilder()
                .setProcessorName(processorName)
                .build();
        return new EventProcessorEvents.EventProcessorStatusUpdated(new ClientEventProcessorInfo(
                "", "", context, component, info
        ), false);
    }

    private List<ClientProcessor> generateProcessors() {
        List<ClientProcessor> processors = new LinkedList<>();
        Arrays.asList("contextOne", "contextTwo", "contextThree").forEach(context -> {
            Arrays.asList("componentOne", "componentTwo", "componentThree").forEach(component -> {
                Arrays.asList("processorOne", "processorTwo", "processorThree").forEach(name -> {
                    processors.add(createProcessor(context, component, name, true));
                    processors.add(createProcessor(context, component, name, false));
                });
            });
        });
        return processors;
    }
}
