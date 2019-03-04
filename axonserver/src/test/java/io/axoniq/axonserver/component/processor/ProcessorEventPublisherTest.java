package io.axoniq.axonserver.component.processor;

import io.axoniq.axonserver.applicationevents.EventProcessorEvents.MergeSegmentRequest;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.ReleaseSegmentRequest;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.SplitSegmentRequest;
import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.component.processor.listener.ClientProcessors;
import io.axoniq.axonserver.grpc.PlatformService;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo.EventTrackerInfo;
import org.assertj.core.util.Lists;
import org.junit.*;
import org.mockito.*;
import org.springframework.context.ApplicationEventPublisher;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Test whether the public API of the {@link ProcessorEventPublisher} publishes application events as expected.
 * Thus, the application events contain the fields as provided through the functions.
 *
 * @author Steven van Beelen
 */
public class ProcessorEventPublisherTest {

    private static final String CLIENT_NAME_OF_SPLIT = "splitClientName";
    private static final String PROCESSOR_NAME_TO_SPLIT = "splitProcessorName";
    private static final int SEGMENT_ID_TO_SPLIT = 1;

    private static final String CLIENT_NAME_OF_MERGE = "mergeClientName";
    private static final String PROCESSOR_NAME_TO_MERGE = "mergeProcessorName";
    private static final int SEGMENT_ID_TO_MERGE = 0;
    private static final int PAIRED_WITH_SEGMENT = 4;

    private static final List<String> CLIENTS = Lists.newArrayList(CLIENT_NAME_OF_SPLIT, CLIENT_NAME_OF_MERGE);

    private final PlatformService platformService = mock(PlatformService.class);
    private final ApplicationEventPublisher applicationEventPublisher = mock(ApplicationEventPublisher.class);
    private final ClientProcessors clientProcessors = mock(ClientProcessors.class);

    private ProcessorEventPublisher testSubject;

    @Before
    public void setUp() {
        List<EventTrackerInfo> segmentInfo = new ArrayList<>();
        int biggestSegment = 4; // Biggest, as it's only one/fourth of the event stream
        int smallestSegment = 8; // Smallest, as it's one/sixteenth of the event stream
        segmentInfo.add(EventTrackerInfo.newBuilder()
                                        .setSegmentId(SEGMENT_ID_TO_MERGE)
                                        .setOnePartOf(smallestSegment)
                                        .build());
        segmentInfo.add(EventTrackerInfo.newBuilder()
                                        .setSegmentId(SEGMENT_ID_TO_SPLIT)
                                        .setOnePartOf(biggestSegment)
                                        .build());
        segmentInfo.add(EventTrackerInfo.newBuilder().setSegmentId(2).setOnePartOf(biggestSegment).build());
        segmentInfo.add(EventTrackerInfo.newBuilder().setSegmentId(3).setOnePartOf(biggestSegment).build());
        segmentInfo.add(EventTrackerInfo.newBuilder()
                                        .setSegmentId(PAIRED_WITH_SEGMENT)
                                        .setOnePartOf(smallestSegment)
                                        .build());

        List<ClientProcessor> eventProcessors = new ArrayList<>();

        ClientProcessor splitClientProcessor = mock(ClientProcessor.class);
        when(splitClientProcessor.clientId()).thenReturn(CLIENT_NAME_OF_SPLIT);
        EventProcessorInfo eventProcessorToSplit = EventProcessorInfo.newBuilder()
                                                                     .setProcessorName(PROCESSOR_NAME_TO_SPLIT)
                                                                     .addAllEventTrackersInfo(segmentInfo)
                                                                     .build();
        when(splitClientProcessor.eventProcessorInfo()).thenReturn(eventProcessorToSplit);
        eventProcessors.add(splitClientProcessor);

        ClientProcessor mergeClientProcessor = mock(ClientProcessor.class);
        when(mergeClientProcessor.clientId()).thenReturn(CLIENT_NAME_OF_MERGE);
        EventProcessorInfo eventProcessorToMerge = EventProcessorInfo.newBuilder()
                                                                     .setProcessorName(PROCESSOR_NAME_TO_MERGE)
                                                                     .addAllEventTrackersInfo(segmentInfo)
                                                                     .build();
        when(mergeClientProcessor.eventProcessorInfo()).thenReturn(eventProcessorToMerge);
        eventProcessors.add(mergeClientProcessor);

        when(clientProcessors.spliterator()).thenReturn(eventProcessors.spliterator());

        testSubject = new ProcessorEventPublisher(
                platformService, applicationEventPublisher, clientProcessors
        );
    }

    /**
     * Test whether the {@link ProcessorEventPublisher#splitSegment(List, String)} operation correctly
     * deduces what the largest Segment for a given Event Processor is, for which the {@code segmentId} will be included
     * to the {@link SplitSegmentRequest}.
     */
    @Test
    public void testSplitSegmentSelectsTheLargestSegmentToSplit() {
        testSubject.splitSegment(CLIENTS, PROCESSOR_NAME_TO_SPLIT);

        ArgumentCaptor<SplitSegmentRequest> argumentCaptor = ArgumentCaptor.forClass(SplitSegmentRequest.class);
        verify(applicationEventPublisher).publishEvent(argumentCaptor.capture());
        SplitSegmentRequest result = argumentCaptor.getValue();

        assertFalse(result.isProxied());
        assertEquals(CLIENT_NAME_OF_SPLIT, result.getClientName());
        assertEquals(PROCESSOR_NAME_TO_SPLIT, result.getProcessorName());
        assertEquals(SEGMENT_ID_TO_SPLIT, result.getSegmentId());
    }

    /**
     * Test whether the {@link ProcessorEventPublisher#mergeSegment(List, String)} operation correctly
     * deduces what the smallest Segment for a given Event Processor is, for which the {@code segmentId} will be
     * included to the {@link MergeSegmentRequest}.
     */
    @Test
    public void testMergeSegmentSelectsTheSmallestSegmentToMerge() {
        int expectedInvocations = 2;

        testSubject.mergeSegment(CLIENTS, PROCESSOR_NAME_TO_MERGE);

        ArgumentCaptor<Object> argumentCaptor = ArgumentCaptor.forClass(Object.class);
        verify(applicationEventPublisher, times(expectedInvocations)).publishEvent(argumentCaptor.capture());
        List<Object> publishedEvents = argumentCaptor.getAllValues();

        ReleaseSegmentRequest releaseRequest = (ReleaseSegmentRequest) publishedEvents.get(0);
        assertFalse(releaseRequest.isProxied());
        assertEquals(CLIENT_NAME_OF_SPLIT, releaseRequest.getClientName());
        assertEquals(PROCESSOR_NAME_TO_MERGE, releaseRequest.getProcessorName());
        assertEquals(PAIRED_WITH_SEGMENT, releaseRequest.getSegmentId());

        MergeSegmentRequest mergeRequest = (MergeSegmentRequest) publishedEvents.get(1);
        assertFalse(mergeRequest.isProxied());
        assertEquals(CLIENT_NAME_OF_MERGE, mergeRequest.getClientName());
        assertEquals(PROCESSOR_NAME_TO_MERGE, mergeRequest.getProcessorName());
        assertEquals(SEGMENT_ID_TO_MERGE, mergeRequest.getSegmentId());
    }
}