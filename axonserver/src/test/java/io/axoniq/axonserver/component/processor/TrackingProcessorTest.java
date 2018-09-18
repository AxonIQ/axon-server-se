package io.axoniq.axonserver.component.processor;

import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.component.processor.listener.FakeClientProcessor;
import io.axoniq.axonserver.serializer.GsonMedia;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import org.junit.*;

import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * Created by Sara Pellegrini on 01/08/2018.
 * sara.pellegrini@gmail.com
 */
public class TrackingProcessorTest {

    @Test
    public void printOn() {
        EventProcessorInfo.EventTrackerInfo trackerInfo0 = EventProcessorInfo.EventTrackerInfo.newBuilder()
                                                                                              .setCaughtUp(true)
                                                                                              .setReplaying(false)
                                                                                              .setOnePartOf(2)
                                                                                              .setSegmentId(0).build();
        EventProcessorInfo processorInfo0 = EventProcessorInfo.newBuilder()
                                                              .setMode("Tracking")
                                                              .setActiveThreads(1)
                                                              .setAvailableThreads(3)
                                                              .setRunning(true)
                                                              .addEventTrackersInfo(trackerInfo0).build();

        EventProcessorInfo.EventTrackerInfo trackerInfo1 = EventProcessorInfo.EventTrackerInfo.newBuilder()
                                                                                              .setCaughtUp(true)
                                                                                              .setReplaying(false)
                                                                                              .setOnePartOf(2)
                                                                                              .setSegmentId(1).build();
        EventProcessorInfo processorInfo1 = EventProcessorInfo.newBuilder()
                                                              .setMode("Tracking")
                                                              .setActiveThreads(1)
                                                              .setAvailableThreads(0)
                                                              .setRunning(true)
                                                              .addEventTrackersInfo(trackerInfo1).build();
        List<ClientProcessor> clientProcessors = asList(new FakeClientProcessor("clientIdOne", true, processorInfo0),
                                                        new FakeClientProcessor("clientIdTwo", true, processorInfo1)
        );
        TrackingProcessor testSubject = new TrackingProcessor("processor name", "tracking", clientProcessors);
        GsonMedia media = new GsonMedia();
        testSubject.printOn(media);
        System.out.println(media.toString());
        String expected =
                "{\"name\":\"processor name\","
                        + "\"mode\":\"tracking\","
                        + "\"warnings\":[],"
                        + "\"freeThreadInstances\":[\"clientIdOne\"],"
                        + "\"activeThreads\":2,"
                        + "\"canPause\":true,"
                        + "\"canPlay\":false,"
                        + "\"trackers\":["
                        + "{\"clientId\":\"clientIdOne\",\"segmentId\":0,\"caughtUp\":true,\"replaying\":false,\"onePartOf\":2},"
                        + "{\"clientId\":\"clientIdTwo\",\"segmentId\":1,\"caughtUp\":true,\"replaying\":false,\"onePartOf\":2}"
                        + "]}";
        assertEquals(expected, media.toString());
    }
}