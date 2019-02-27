package io.axoniq.axonserver.enterprise.cluster;

import io.axoniq.axonserver.applicationevents.EventProcessorEvents.MergeSegmentRequest;
import io.axoniq.axonserver.applicationevents.EventProcessorEvents.SplitSegmentRequest;
import io.axoniq.axonserver.grpc.internal.ClientEventProcessorSegment;
import io.axoniq.axonserver.grpc.internal.ConnectorCommand;
import org.junit.*;
import org.mockito.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Tests whether the event listeners in the {@link EventProcessorSynchronizer} actually republish the handled event as a
 * {@link ConnectorCommand} over the given {@link ClusterPublisher}. Should assert whether the republished message
 * contains the expected fields as well.
 *
 * @author Steven van Beelen
 */
public class EventProcessorSynchronizerTest {

    private static final boolean PROXIED = true;
    private static final String CLIENT_NAME = "clientName";
    private static final String PROCESSOR_NAME = "processorName";
    private static final int SEGMENT_ID = 1;

    private final ClusterPublisher clusterPublisher = mock(ClusterPublisher.class);

    private final EventProcessorSynchronizer testSubject = new EventProcessorSynchronizer(clusterPublisher);

    @Test
    public void testOnSplitSegmentRequestASplitSegmentConnectorCommandIsPublished() {
        testSubject.on(new SplitSegmentRequest(PROXIED, CLIENT_NAME, PROCESSOR_NAME, SEGMENT_ID));

        ArgumentCaptor<ConnectorCommand> argumentCaptor = ArgumentCaptor.forClass(ConnectorCommand.class);
        verify(clusterPublisher).publish(argumentCaptor.capture());
        ConnectorCommand result = argumentCaptor.getValue();

        ClientEventProcessorSegment splitSegmentRequest = result.getSplitSegment();
        assertNotNull(splitSegmentRequest);
        assertEquals(CLIENT_NAME, splitSegmentRequest.getClient());
        assertEquals(PROCESSOR_NAME, splitSegmentRequest.getProcessorName());
        assertEquals(SEGMENT_ID, splitSegmentRequest.getSegmentIdentifier());
    }

    @Test
    public void testOnMergeSegmentRequestAMergeSegmentConnectorCommandIsPublished() {
        testSubject.on(new MergeSegmentRequest(PROXIED, CLIENT_NAME, PROCESSOR_NAME, SEGMENT_ID));

        ArgumentCaptor<ConnectorCommand> argumentCaptor = ArgumentCaptor.forClass(ConnectorCommand.class);
        verify(clusterPublisher).publish(argumentCaptor.capture());
        ConnectorCommand result = argumentCaptor.getValue();

        ClientEventProcessorSegment mergeSegmentRequest = result.getMergeSegment();
        assertNotNull(mergeSegmentRequest);
        assertEquals(CLIENT_NAME, mergeSegmentRequest.getClient());
        assertEquals(PROCESSOR_NAME, mergeSegmentRequest.getProcessorName());
        assertEquals(SEGMENT_ID, mergeSegmentRequest.getSegmentIdentifier());
    }
}