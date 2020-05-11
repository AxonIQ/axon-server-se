package io.axoniq.axonserver.component.processor;

import io.axoniq.axonserver.applicationevents.EventProcessorEvents.MergeSegmentRequest;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.MergeSegmentsSucceeded;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.ReleaseSegmentRequest;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.SplitSegmentRequest;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.SplitSegmentsSucceeded;
import io.axoniq.axonserver.grpc.InstructionResult;
import io.axoniq.axonserver.grpc.control.EventProcessorSegmentReference;
import io.axoniq.axonserver.grpc.control.PlatformOutboundInstruction;
import io.axoniq.axonserver.grpc.istruction.result.InstructionResultSource.ResultSubscriber;
import org.junit.*;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.axoniq.axonserver.grpc.control.PlatformOutboundInstruction.newBuilder;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link EventProcessorService}.
 *
 * @author Sara Pellegrini
 */
public class EventProcessorServiceTest {

    private Map<String, List<PlatformOutboundInstruction>> publishedInstructions = new ConcurrentHashMap<>();

    private Map<String, List<ResultSubscriber>> resultSubscribers = new ConcurrentHashMap<>();

    private List<Object> publishedInternalEvents = new CopyOnWriteArrayList<>();

    private EventProcessorService testSubject = new EventProcessorService(
            (client, i) -> publishedInstructions.computeIfAbsent(client, k -> new CopyOnWriteArrayList<>()).add(i),
            instructionId -> (subscriber, timeout) -> resultSubscribers.computeIfAbsent(instructionId,
                                                                                        k -> new CopyOnWriteArrayList<>())
                                                                       .add(subscriber),
            event -> publishedInternalEvents.add(event));

    @Before
    public void setUp() throws Exception {
        publishedInternalEvents.clear();
        resultSubscribers.clear();
        publishedInstructions.clear();
    }

    @Test
    public void onMergeSegmentRequestExecuted() {
        MergeSegmentRequest mergeSegmentRequest = new MergeSegmentRequest(false,
                                                                          "MergeClient",
                                                                          "Processor",
                                                                          1);
        testSubject.on(mergeSegmentRequest);
        assertFalse(publishedInstructions.isEmpty());
        PlatformOutboundInstruction published = publishedInstructions.get("MergeClient").get(0);
        PlatformOutboundInstruction expected = newBuilder()
                .setMergeEventProcessorSegment(
                        EventProcessorSegmentReference.newBuilder()
                                                      .setSegmentIdentifier(1)
                                                      .setProcessorName("Processor"))
                .build();
        assertEquals(expected, published);

        notifySuccessForInstruction(published.getInstructionId());
        assertFalse(publishedInternalEvents.isEmpty());
        Object event = publishedInternalEvents.get(0);
        assertTrue(event instanceof MergeSegmentsSucceeded);
        assertEquals("Processor", ((MergeSegmentsSucceeded) event).processorName());
        assertEquals("MergeClient", ((MergeSegmentsSucceeded) event).clientName());
    }

    @Test
    public void onMergeSegmentRequestNotExecuted() {
        MergeSegmentRequest mergeSegmentRequest = new MergeSegmentRequest(false,
                                                                          "MergeClient",
                                                                          "Processor",
                                                                          1);
        testSubject.on(mergeSegmentRequest);
        assertFalse(publishedInstructions.isEmpty());
        PlatformOutboundInstruction published = publishedInstructions.get("MergeClient").get(0);
        PlatformOutboundInstruction expected = newBuilder()
                .setMergeEventProcessorSegment(
                        EventProcessorSegmentReference.newBuilder()
                                                      .setSegmentIdentifier(1)
                                                      .setProcessorName("Processor"))
                .build();
        assertEquals(expected, published);

        notifyFailureForInstruction(published.getInstructionId());
        assertTrue(publishedInternalEvents.isEmpty());
    }

    @Test
    public void onSplitSegmentRequestExecuted() {
        SplitSegmentRequest splitSegmentRequest = new SplitSegmentRequest(false,
                                                                          "SplitClient",
                                                                          "processor",
                                                                          1);
        testSubject.on(splitSegmentRequest);
        assertFalse(publishedInstructions.isEmpty());
        PlatformOutboundInstruction published = publishedInstructions.get("SplitClient").get(0);
        PlatformOutboundInstruction expected = newBuilder()
                .setSplitEventProcessorSegment(
                        EventProcessorSegmentReference.newBuilder()
                                                      .setSegmentIdentifier(1)
                                                      .setProcessorName("processor"))
                .build();
        assertEquals(expected, published);

        notifySuccessForInstruction(published.getInstructionId());
        assertFalse(publishedInternalEvents.isEmpty());
        Object event = publishedInternalEvents.get(0);
        assertTrue(event instanceof SplitSegmentsSucceeded);
        assertEquals("processor", ((SplitSegmentsSucceeded) event).processorName());
        assertEquals("SplitClient", ((SplitSegmentsSucceeded) event).clientName());
    }

    @Test
    public void onSplitSegmentRequestNotExecuted() {
        SplitSegmentRequest splitSegmentRequest = new SplitSegmentRequest(false,
                                                                          "SplitClient",
                                                                          "processor",
                                                                          1);
        testSubject.on(splitSegmentRequest);
        assertFalse(publishedInstructions.isEmpty());
        PlatformOutboundInstruction published = publishedInstructions.get("SplitClient").get(0);
        PlatformOutboundInstruction expected = newBuilder()
                .setSplitEventProcessorSegment(
                        EventProcessorSegmentReference.newBuilder()
                                                      .setSegmentIdentifier(1)
                                                      .setProcessorName("processor"))
                .build();
        assertEquals(expected, published);

        notifyFailureForInstruction(published.getInstructionId());
        assertTrue(publishedInternalEvents.isEmpty());
    }

    @Test
    public void onReleaseSegmentRequest() {
        ReleaseSegmentRequest releaseSegmentRequest = new ReleaseSegmentRequest("Release",
                                                                                "processor",
                                                                                1,
                                                                                false);
        testSubject.on(releaseSegmentRequest);
        assertFalse(publishedInstructions.isEmpty());
        PlatformOutboundInstruction published = publishedInstructions.get("Release").get(0);
        PlatformOutboundInstruction expected = newBuilder()
                .setReleaseSegment(
                        EventProcessorSegmentReference.newBuilder()
                                                      .setSegmentIdentifier(1)
                                                      .setProcessorName("processor"))
                .build();
        assertEquals(expected, published);
    }

    private void notifySuccessForInstruction(String instructionId) {
        InstructionResult success = InstructionResult.newBuilder()
                                                     .setInstructionId(instructionId)
                                                     .setSuccess(true)
                                                     .build();
        resultSubscribers.getOrDefault(instructionId, Collections.emptyList())
                         .forEach(resultSubscriber -> resultSubscriber.onResult(success));
    }

    private void notifyFailureForInstruction(String instructionId) {
        InstructionResult failure = InstructionResult.newBuilder()
                                                     .setInstructionId(instructionId)
                                                     .setSuccess(false)
                                                     .build();
        resultSubscribers.getOrDefault(instructionId, Collections.emptyList())
                         .forEach(resultSubscriber -> resultSubscriber.onResult(failure));
    }
}
