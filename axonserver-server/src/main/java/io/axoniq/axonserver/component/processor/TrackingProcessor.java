package io.axoniq.axonserver.component.processor;

import io.axoniq.axonserver.component.processor.listener.ClientProcessor;
import io.axoniq.axonserver.component.processor.warning.DuplicatedTrackers;
import io.axoniq.axonserver.component.processor.warning.MissingTrackers;
import io.axoniq.axonserver.component.processor.warning.Warning;
import io.axoniq.axonserver.serializer.Media;
import io.axoniq.axonserver.serializer.Printable;
import io.axoniq.axonserver.grpc.control.EventProcessorInfo;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.concat;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

/**
 * Created by Sara Pellegrini on 14/03/2018.
 * sara.pellegrini@gmail.com
 */
public class TrackingProcessor extends GenericProcessor implements EventProcessor {

    TrackingProcessor(String name, String mode,
                      Collection<ClientProcessor> processors) {
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
        media.withStrings("freeThreadInstances", freeThreadInstances)
             .with("activeThreads", activeThreads)
             .with("canPause", processors().stream().anyMatch(ClientProcessor::running))
             .with("canPlay", processors().stream().anyMatch(p -> !p.running()))
             .with("trackers", trackers());
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
