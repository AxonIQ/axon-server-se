package io.axoniq.axonserver.component.processor;

import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.component.processor.listener.FakeClientProcessor;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import io.axoniq.axonserver.serializer.GsonMedia;
import org.junit.*;

import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * @author Sara Pellegrini
 */
public class TrackingProcessorTest {

    @Test
    public void testPrintOnCreatesFullyFledgedJson() {
        String expectedJson =
                "{\"name\":\"processor name\","
                        + "\"mode\":\"tracking\","
                        + "\"warnings\":[],"
                        + "\"freeThreadInstances\":[\"clientIdOne\"],"
                        + "\"activeThreads\":2,"
                        + "\"canPause\":true,"
                        + "\"canPlay\":false,"
                        + "\"canSplit\":true,"
                        + "\"canMerge\":true,"
                        + "\"trackers\":["
                        + "{\"clientId\":\"clientIdOne\",\"segmentId\":0,\"caughtUp\":true,\"replaying\":false,\"onePartOf\":2},"
                        + "{\"clientId\":\"clientIdTwo\",\"segmentId\":1,\"caughtUp\":true,\"replaying\":false,\"onePartOf\":2}"
                        + "]}";

        EventProcessorInfo.EventTrackerInfo trackerInfo0 = EventProcessorInfo.EventTrackerInfo.newBuilder()
                                                                                              .setCaughtUp(true)
                                                                                              .setReplaying(false)
                                                                                              .setOnePartOf(2)
                                                                                              .setSegmentId(0)
                                                                                              .build();
        EventProcessorInfo processorInfo0 = EventProcessorInfo.newBuilder()
                                                              .setMode("Tracking")
                                                              .setActiveThreads(1)
                                                              .setAvailableThreads(3)
                                                              .setRunning(true)
                                                              .addEventTrackersInfo(trackerInfo0)
                                                              .build();

        EventProcessorInfo.EventTrackerInfo trackerInfo1 = EventProcessorInfo.EventTrackerInfo.newBuilder()
                                                                                              .setCaughtUp(true)
                                                                                              .setReplaying(false)
                                                                                              .setOnePartOf(2)
                                                                                              .setSegmentId(1)
                                                                                              .build();
        EventProcessorInfo processorInfo1 = EventProcessorInfo.newBuilder()
                                                              .setMode("Tracking")
                                                              .setActiveThreads(1)
                                                              .setAvailableThreads(0)
                                                              .setRunning(true)
                                                              .addEventTrackersInfo(trackerInfo1)
                                                              .build();
        List<ClientProcessor> testClientProcessors = asList(
                new FakeClientProcessor("clientIdOne", true, processorInfo0),
                new FakeClientProcessor("clientIdTwo", true, processorInfo1)
        );

        TrackingProcessor testSubject = new TrackingProcessor("processor name", "tracking", testClientProcessors);

        GsonMedia media = new GsonMedia();
        testSubject.printOn(media);

        assertEquals(expectedJson, media.toString());
    }

    @Test
    public void testPrintOnDisableCanMergeIfThereIsOnlyOneSegment() {
        String expectedJson =
                "{\"name\":\"processor name\","
                        + "\"mode\":\"tracking\","
                        + "\"warnings\":[],"
                        + "\"freeThreadInstances\":[\"clientIdOne\"],"
                        + "\"activeThreads\":1,"
                        + "\"canPause\":true,"
                        + "\"canPlay\":false,"
                        + "\"canSplit\":true,"
                        + "\"canMerge\":false,"
                        + "\"trackers\":["
                        + "{\"clientId\":\"clientIdOne\",\"segmentId\":0,\"caughtUp\":true,\"replaying\":false,\"onePartOf\":1}"
                        + "]}";

        EventProcessorInfo.EventTrackerInfo trackerInfo0 = EventProcessorInfo.EventTrackerInfo.newBuilder()
                                                                                              .setCaughtUp(true)
                                                                                              .setReplaying(false)
                                                                                              .setOnePartOf(1)
                                                                                              .setSegmentId(0)
                                                                                              .build();
        EventProcessorInfo processorInfo0 = EventProcessorInfo.newBuilder()
                                                              .setMode("Tracking")
                                                              .setActiveThreads(1)
                                                              .setAvailableThreads(1)
                                                              .setRunning(true)
                                                              .addEventTrackersInfo(trackerInfo0)
                                                              .build();
        List<ClientProcessor> testClientProcessors =
                Collections.singletonList(new FakeClientProcessor("clientIdOne", true, processorInfo0));

        TrackingProcessor testSubject = new TrackingProcessor("processor name", "tracking", testClientProcessors);

        GsonMedia media = new GsonMedia();
        testSubject.printOn(media);

        assertEquals(expectedJson, media.toString());
    }
}