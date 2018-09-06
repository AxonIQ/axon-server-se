package io.axoniq.axonhub.component.processor;

import io.axoniq.axonhub.serializer.GsonMedia;
import io.axoniq.platform.grpc.EventProcessorInfo.EventTrackerInfo;
import org.junit.*;

import static io.axoniq.platform.grpc.EventProcessorInfo.EventTrackerInfo.newBuilder;
import static org.junit.Assert.*;

/**
 * Created by Sara Pellegrini on 23/03/2018.
 * sara.pellegrini@gmail.com
 */
public class TrackingProcessorSegmentTest {

    @Test
    public void printOn() {
        GsonMedia gsonMedia = new GsonMedia();
        EventTrackerInfo eventTrackerInfo = newBuilder().setSegmentId(1)
                                                        .setOnePartOf(2)
                                                        .setReplaying(false)
                                                        .setCaughtUp(true)
                                                        .build();
        TrackingProcessorSegment tracker = new TrackingProcessorSegment("myClient", eventTrackerInfo);
        tracker.printOn(gsonMedia);
        assertEquals("{\"clientId\":\"myClient\",\"segmentId\":1,\"caughtUp\":true,\"replaying\":false,\"onePartOf\":2}",
                    gsonMedia.toString());
    }
}