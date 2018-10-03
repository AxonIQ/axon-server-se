package io.axoniq.axonserver.enterprise.cluster.manager;

import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.grpc.internal.ConnectorCommand;
import io.axoniq.axonserver.grpc.internal.NodeContextInfo;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Author: marc
 */
@Component
public class LeaderRequestHandler {
    private final Function<String, String> currentMasterProvider;
    private final BiConsumer<String, ConnectorCommand> commandPublisher;
    private final String nodeName;
    private final Function<String, Long> lastTokenProvider;
    private final Function<String, Integer> nrOfMasterContextsProvider;
    private final Logger logger = LoggerFactory.getLogger(LeaderRequestHandler.class);


    @Autowired
    public LeaderRequestHandler(LocalEventStore localEventStore, Optional<EventStoreManager> eventStoreManager, ClusterController clusterConfiguration) {
        this.nodeName = clusterConfiguration.getName();
        if( eventStoreManager.isPresent()) {
            this.currentMasterProvider = eventStoreManager.get()::getMaster;
            this.nrOfMasterContextsProvider = eventStoreManager.get()::getNrOrMasterContexts;
        } else {
            this.currentMasterProvider = context -> nodeName;
            this.nrOfMasterContextsProvider = context -> 0;
        }
        this.commandPublisher = clusterConfiguration::publishTo;
        this.lastTokenProvider = localEventStore::getLastToken;
    }

    public LeaderRequestHandler(String nodeName,
                                Function<String, String> currentMasterProvider,
                                BiConsumer<String, ConnectorCommand> commandPublisher,
                                Function<String, Long> lastTokenProvider,
                                Function<String, Integer> nrOfMasterContextsProvider) {
        this.currentMasterProvider = currentMasterProvider;
        this.commandPublisher = commandPublisher;
        this.nodeName = nodeName;
        this.lastTokenProvider = lastTokenProvider;
        this.nrOfMasterContextsProvider = nrOfMasterContextsProvider;
    }

    @EventListener
    public void on(RequestLeaderEvent requestLeaderEvent) {
        logger.warn("Received requestLeader {}", requestLeaderEvent.getRequest());
        NodeContextInfo candidate = requestLeaderEvent.getRequest();
        String currentMaster = currentMasterProvider.apply(candidate.getContext());
        if (currentMaster != null) {
            requestLeaderEvent.getCallback().accept(false);
            if( nodeName.equals(currentMaster)) {
                commandPublisher.accept(requestLeaderEvent.getRequest().getNodeName(),
                                        ConnectorCommand.newBuilder()
                                                        .setMasterConfirmation(NodeContextInfo.newBuilder()
                                                                                              .setContext(candidate.getContext())
                                                                                              .setNodeName(currentMasterProvider.apply(candidate.getContext()))
                                                                                              .build())
                                                        .build());
            }
            return;
        }
        requestLeaderEvent.getCallback().accept(checkOnFields(nodeName, candidate));
    }

    private boolean checkOnFields(String me, NodeContextInfo candidate) {
        long sequenceNumber =  lastTokenProvider.apply(candidate.getContext());
        if (candidate.getMasterSequenceNumber() != sequenceNumber)
            return candidate.getMasterSequenceNumber() > sequenceNumber;

        int nrMasterContexts = nrOfMasterContextsProvider.apply(me);

        if (candidate.getNrOfMasterContexts() != nrMasterContexts)
            return candidate.getNrOfMasterContexts() < nrMasterContexts;

        int hashKey = EventStoreManager.hash(candidate.getContext(), me);
        if (candidate.getHashKey() != hashKey)
            return candidate.getHashKey() < hashKey;

        return me.compareTo(candidate.getNodeName()) < 0;
    }

}
