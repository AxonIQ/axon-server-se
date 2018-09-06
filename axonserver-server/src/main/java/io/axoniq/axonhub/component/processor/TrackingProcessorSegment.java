package io.axoniq.axonhub.component.processor;

import io.axoniq.axonhub.serializer.Media;
import io.axoniq.axonhub.serializer.Printable;
import io.axoniq.platform.grpc.EventProcessorInfo;

/**
 * Created by Sara Pellegrini on 21/03/2018.
 * sara.pellegrini@gmail.com
 */
public class TrackingProcessorSegment implements Printable {

    private final String clientId;

    private final EventProcessorInfo.EventTrackerInfo eventTrackerInfo;

    public TrackingProcessorSegment(String clientId, EventProcessorInfo.EventTrackerInfo eventTrackerInfo) {
        this.clientId = clientId;
        this.eventTrackerInfo = eventTrackerInfo;
    }

    @Override
    public void printOn(Media media) {
        media.with("clientId", clientId)
             .with("segmentId", eventTrackerInfo.getSegmentId())
             .with("caughtUp", eventTrackerInfo.getCaughtUp())
             .with("replaying", eventTrackerInfo.getReplaying())
             .with("onePartOf", eventTrackerInfo.getOnePartOf());
    }
}