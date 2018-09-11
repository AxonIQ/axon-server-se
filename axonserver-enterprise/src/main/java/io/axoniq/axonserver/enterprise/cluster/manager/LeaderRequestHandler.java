package io.axoniq.axonserver.enterprise.cluster.manager;

import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonhub.internal.grpc.ConnectorCommand;
import io.axoniq.axonhub.internal.grpc.NodeContextInfo;
import io.axoniq.axonserver.enterprise.cluster.manager.EventStoreManager;
import io.axoniq.axonserver.enterprise.cluster.manager.RequestLeaderEvent;
import io.axoniq.axonserver.localstorage.LocalEventStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Author: marc
 */
@Component
public class LeaderRequestHandler {
    private final Function<String, String> currentMasterProvider;
    private final Consumer<ConnectorCommand> commandPublisher;
    private final String nodeName;
    private final Function<String, Long> lastTokenProvider;
    private final Function<String, Integer> nrOfMasterContextsProvider;


    @Autowired
    public LeaderRequestHandler(LocalEventStore localEventStore, EventStoreManager eventStoreManager, ClusterController clusterConfiguration) {
        this(
                clusterConfiguration.getName(),
                eventStoreManager::getMaster,
                clusterConfiguration::publish,
                localEventStore::getLastToken,
                eventStoreManager::getNrOrMasterContexts
        );
    }

    public LeaderRequestHandler(String nodeName,
                                Function<String, String> currentMasterProvider,
                                Consumer<ConnectorCommand> commandPublisher,
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
        NodeContextInfo candidate = requestLeaderEvent.getRequest();
        if (currentMasterProvider.apply(candidate.getContext()) != null) {
            requestLeaderEvent.getCallback().accept(false);
            commandPublisher.accept(ConnectorCommand.newBuilder().setMasterConfirmation(NodeContextInfo.newBuilder()
                                                                                                            .setContext(candidate.getContext())
                                                                                                            .setNodeName(currentMasterProvider.apply(candidate.getContext()))
                                                                                                            .build()).build());
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
