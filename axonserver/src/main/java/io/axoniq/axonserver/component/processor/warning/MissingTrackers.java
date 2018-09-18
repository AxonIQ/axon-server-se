package io.axoniq.axonserver.component.processor.warning;

import io.axoniq.axonserver.grpc.control.EventProcessorInfo.EventTrackerInfo;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by Sara Pellegrini on 22/03/2018.
 * sara.pellegrini@gmail.com
 */
public class MissingTrackers implements Warning {

    private final Iterable<EventTrackerInfo> trackerInfos;

    public MissingTrackers(Iterable<EventTrackerInfo> trackerInfos) {
        this.trackerInfos = trackerInfos;
    }

    @Override
    public boolean active() {
        double completion = 0;
        Set<Integer> ids = new HashSet<>();

        for (EventTrackerInfo info : trackerInfos) {
            int segmentId = info.getSegmentId();
            if (!ids.contains(segmentId))completion += 1d/info.getOnePartOf();
            ids.add(segmentId);
        }

        return completion != 1;
    }

    @Override
    public String message() {
        return "Not all segments claimed";
    }

}
