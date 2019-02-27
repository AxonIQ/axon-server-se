package io.axoniq.axonserver.component.processor;

import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.component.processor.warning.DuplicatedTrackers;
import io.axoniq.axonserver.component.processor.warning.MissingTrackers;
import io.axoniq.axonserver.component.processor.warning.Warning;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;
import io.axoniq.axonserver.serializer.Media;
import io.axoniq.axonserver.serializer.Printable;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.concat;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

/**
 * Tracking Event Processor state representation for the UI.
 *
 * @author Sara Pellegrini
 * @since 4.0
 */
public class TrackingProcessor extends GenericProcessor implements EventProcessor {

    private static final String FREE_THREAD_INSTANCES_COUNT = "freeThreadInstances";
    private static final String ACTIVE_THREADS_COUNT = "activeThreads";
    private static final String CAN_PAUSE_KEY = "canPause";
    private static final String CAN_PLAY_KEY = "canPlay";
    private static final String CAN_SPLIT_KEY = "canSplit";
    private static final String CAN_MERGE_KEY = "canMerge";
    private static final String TRACKERS_LIST_KEY = "trackers";

    TrackingProcessor(String name, String mode, Collection<ClientProcessor> processors) {
        super(name, mode, processors);
    }

    @Override
    public Iterable<Warning> warnings() {
        return asList(
                new DuplicatedTrackers(concat(processors())),
                new MissingTrackers(concat(processors()))
        );
    }

    @Override
    public void printOn(Media media) {
        EventProcessor.super.printOn(media);
        Integer activeThreads = processorInstances().stream().map(EventProcessorInfo::getActiveThreads)
                                                    .reduce(Integer::sum).orElse(0);
        Set<String> freeThreadInstances = processors().stream()
                                                      .filter(p -> p.eventProcessorInfo().getAvailableThreads() > 0)
                                                      .map(ClientProcessor::clientId)
                                                      .collect(Collectors.toSet());

        boolean isRunning = processors().stream().anyMatch(ClientProcessor::running);
        media.withStrings(FREE_THREAD_INSTANCES_COUNT, freeThreadInstances)
             .with(ACTIVE_THREADS_COUNT, activeThreads)
             .with(CAN_PAUSE_KEY, isRunning)
             .with(CAN_PLAY_KEY, processors().stream().anyMatch(p -> !p.running()))
             .with(CAN_SPLIT_KEY, isRunning)
             .with(CAN_MERGE_KEY, isRunning)
             .with(TRACKERS_LIST_KEY, trackers());
    }

    private List<Printable> trackers() {
        return processors().stream()
                           .flatMap(client -> client.eventProcessorInfo().getEventTrackersInfoList()
                                                    .stream()
                                                    .map(tracker -> new TrackingProcessorSegment(client.clientId(),
                                                                                                 tracker))
                           ).collect(toList());
    }

    private List<EventProcessorInfo> processorInstances() {
        return processors().stream().map(ClientProcessor::eventProcessorInfo).collect(toList());
    }
}
