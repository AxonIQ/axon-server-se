package io.axoniq.axonserver.component.processor.balancing.strategy;

import io.axoniq.axonserver.component.processor.balancing.SameProcessor;
import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;
import io.axoniq.axonserver.component.processor.listener.ClientProcessors;
import io.axoniq.platform.grpc.EventProcessorInfo;
import io.axoniq.platform.grpc.EventProcessorInfo.EventTrackerInfo;

import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toSet;
import static java.util.stream.StreamSupport.stream;

/**
 * Created by Sara Pellegrini on 10/08/2018.
 * sara.pellegrini@gmail.com
 */
public class DefaultInstancesRepository implements ThreadNumberBalancing.InstancesRepo {

    private final ClientProcessors processors;

    DefaultInstancesRepository(ClientProcessors processors) {
        this.processors = processors;
    }

    @Override
    public Iterable<ThreadNumberBalancing.Application> findFor(TrackingEventProcessor processor) {
        return () -> stream(processors.spliterator(), false)
                .filter(new SameProcessor(processor))
                .map(p -> {
                    EventProcessorInfo i = p.eventProcessorInfo();
                    int threadPoolSize = i.getAvailableThreads() + i.getEventTrackersInfoCount();
                    List<EventTrackerInfo> trackers = i.getEventTrackersInfoList();
                    Set<Integer> segments = trackers.stream().map(EventTrackerInfo::getSegmentId).collect(toSet());
                    return new ThreadNumberBalancing.Application(p.clientId(), threadPoolSize, segments);
                }).iterator();
    }

}
