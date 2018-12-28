package io.axoniq.axonserver.enterprise.cluster.manager;

import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.internal.SyncStatusController;
import io.axoniq.axonserver.grpc.event.Event;
import io.axoniq.axonserver.grpc.internal.ConnectorCommand;
import io.axoniq.axonserver.grpc.internal.NodeContextInfo;
import io.axoniq.axonserver.localstorage.EventType;
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
    private final Function<String, Long> lastCommittedTokenProvider;
    private final Function<String, Long> generationProvider;
    private final Function<String, Integer> nrOfMasterContextsProvider;
    private final Logger logger = LoggerFactory.getLogger(LeaderRequestHandler.class);


    @Autowired
    public LeaderRequestHandler(LocalEventStore localEventStore, SyncStatusController syncStatusController, Optional<EventStoreManager> eventStoreManager, ClusterController clusterConfiguration) {
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
        this.lastCommittedTokenProvider = context -> syncStatusController.getSafePoint(EventType.EVENT, context);
        this.generationProvider = context -> syncStatusController.generation(EventType.EVENT, context);
    }

    public LeaderRequestHandler(String nodeName,
                                Function<String, String> currentMasterProvider,
                                BiConsumer<String, ConnectorCommand> commandPublisher,
                                Function<String, Long> lastTokenProvider,
                                Function<String, Long> lastCommittedTokenProvider,
                                Function<String, Long> generationProvider,
                                Function<String, Integer> nrOfMasterContextsProvider) {
        this.currentMasterProvider = currentMasterProvider;
        this.commandPublisher = commandPublisher;
        this.nodeName = nodeName;
        this.lastTokenProvider = lastTokenProvider;
        this.lastCommittedTokenProvider = lastCommittedTokenProvider;
        this.generationProvider = generationProvider;
        this.nrOfMasterContextsProvider = nrOfMasterContextsProvider;
    }

    @EventListener
    public void on(RequestLeaderEvent requestLeaderEvent) {
        logger.debug("Received requestLeader {}", requestLeaderEvent.getRequest());
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
        //the candidate is not eligible because the sequence is behind the safe point (the node is catching up)
        if (candidate.getEventSafePoint() > candidate.getMasterSequenceNumber() + 1) {
            logger.warn("{} is catching up, returning false", candidate.getNodeName());
            return false;
        }

        long eventSafePoint = lastCommittedTokenProvider.apply(candidate.getContext());
        long sequenceNumber =  lastTokenProvider.apply(candidate.getContext());

        if( eventSafePoint >= sequenceNumber + 1) {
            // I am catching up, allow candidate to become master
            logger.warn("{} is catching up, returning true", me);
            return true;
        }

        if (candidate.getEventSafePoint() != eventSafePoint){
            logger.warn("{} different safepoint, returning {}", candidate.getNodeName(), candidate.getEventSafePoint() > eventSafePoint);
            return candidate.getEventSafePoint() > eventSafePoint;
        }

        //if previous parameter are equals, choose the node that joined the most recent master generation
        long prevMasterGeneration = generationProvider.apply(candidate.getContext());
        if (candidate.getPrevMasterGeneration() != prevMasterGeneration){
            logger.warn("{} different prevMasterGeneration, returning {}", candidate.getNodeName(), candidate.getPrevMasterGeneration() > prevMasterGeneration);
            return candidate.getPrevMasterGeneration() > prevMasterGeneration;
        }


        if (candidate.getMasterSequenceNumber() != sequenceNumber) {
            logger.warn("{} different masterSequenceNumber, returning {}", candidate.getNodeName(), candidate.getMasterSequenceNumber() > sequenceNumber);
            return candidate.getMasterSequenceNumber() > sequenceNumber;
        }

        int nrMasterContexts = nrOfMasterContextsProvider.apply(me);

        if (candidate.getNrOfMasterContexts() != nrMasterContexts)
            return candidate.getNrOfMasterContexts() < nrMasterContexts;

        int hashKey = EventStoreManager.hash(candidate.getContext(), me);
        if (candidate.getHashKey() != hashKey) {
            logger.warn("{} different hashKey, returning {}", candidate.getNodeName(), candidate.getHashKey() < hashKey);
            return candidate.getHashKey() < hashKey;
        }

        return me.compareTo(candidate.getNodeName()) < 0;
    }

}
