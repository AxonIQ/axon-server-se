package io.axoniq.axonserver.component.processor.warning;

import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import org.junit.*;

import java.util.List;

import static io.axoniq.axonserver.grpc.control.EventProcessorInfo.EventTrackerInfo.newBuilder;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * Created by Sara Pellegrini on 23/03/2018.
 * sara.pellegrini@gmail.com
 */
public class MissingTrackersTest {

    @Test
    public void testActive() {
        List<EventProcessorInfo.EventTrackerInfo> eventTrackerInfoList = asList(newBuilder().setSegmentId(1)
                                                                                            .setOnePartOf(2).build(),
                                                                                newBuilder().setSegmentId(2)
                                                                                            .setOnePartOf(4).build());

        MissingTrackers warning = new MissingTrackers(eventTrackerInfoList);
        assertTrue(warning.active());
    }

    @Test
    public void testNotActive() {
        List<EventProcessorInfo.EventTrackerInfo> eventTrackerInfoList = asList(newBuilder().setSegmentId(1)
                                                                                            .setOnePartOf(2).build(),
                                                                                newBuilder().setSegmentId(2)
                                                                                            .setOnePartOf(2).build());

        MissingTrackers warning = new MissingTrackers(eventTrackerInfoList);
        assertFalse(warning.active());
    }

    @Test
    public void testActiveWithDuplicatesSegments() {
        List<EventProcessorInfo.EventTrackerInfo> eventTrackerInfoList = asList(newBuilder().setSegmentId(1)
                                                                                            .setOnePartOf(2).build(),
                                                                                newBuilder().setSegmentId(1)
                                                                                            .setOnePartOf(4).build());

        MissingTrackers warning = new MissingTrackers(eventTrackerInfoList);
        assertTrue(warning.active());
    }

    @Test
    public void testNotActiveWithDuplicatesSegments() {
        List<EventProcessorInfo.EventTrackerInfo> eventTrackerInfoList = asList(newBuilder().setSegmentId(1)
                                                                                            .setOnePartOf(2).build(),
                                                                                newBuilder().setSegmentId(2)
                                                                                            .setOnePartOf(2).build(),
                                                                                newBuilder().setSegmentId(1)
                                                                                            .setOnePartOf(2).build(),
                                                                                newBuilder().setSegmentId(2)
                                                                                            .setOnePartOf(2).build()
        );

        Warning warning = new MissingTrackers(eventTrackerInfoList);
        assertFalse(warning.active());
    }
}