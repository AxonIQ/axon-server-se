package io.axoniq.axonserver.enterprise.cluster.coordinator;

import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents.BecomeCoordinator;
import io.axoniq.axonserver.grpc.Confirmation;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.enterprise.jpa.Context;
import io.axoniq.axonserver.grpc.internal.NodeContext;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static io.axoniq.axonserver.grpc.internal.NodeContext.newBuilder;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Created by Sara Pellegrini on 24/08/2018.
 * sara.pellegrini@gmail.com
 */
@Component
public class CoordinatorElectionProcess {

    private final Logger logger = LoggerFactory.getLogger(CoordinatorElectionProcess.class);

    private final String thisNodeName;

    private final Long waitMilliseconds;

    private final ApplicationEventPublisher eventPublisher;

    private final Sender<NodeContext, ClusterNode, StreamObserver<Confirmation>> sender;

    @Autowired
    public CoordinatorElectionProcess(MessagingPlatformConfiguration messagingPlatformConfiguration,
                                      GrpcRequestCoordinatorSender sender,
                                      ApplicationEventPublisher applicationEventPublisher) {
        this(messagingPlatformConfiguration.getName(),
             messagingPlatformConfiguration.getCluster().getConnectionWaitTime(),
             applicationEventPublisher,
             sender);
    }

    public CoordinatorElectionProcess(String thisNodeName,
                                      Long waitMilliseconds,
                                      ApplicationEventPublisher eventPublisher,
                                      Sender<NodeContext, ClusterNode, StreamObserver<Confirmation>> sender) {
        this.thisNodeName = thisNodeName;
        this.waitMilliseconds = waitMilliseconds;
        this.eventPublisher = eventPublisher;
        this.sender = sender;
    }

    void startElection(Context context, Supplier<Boolean> coordinatorFound) {
        if (context == null) return;
        if (coordinatorFound.get()) return;
        try {
            Thread.sleep(1000);
            if (electionRound(context, coordinatorFound)) return;
            // startElection(context, coordinatorFound);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            // startElection(context, coordinatorFound);
        }
    }

    boolean electionRound(Context context, Supplier<Boolean> coordinatorFound) throws InterruptedException {
        String contextName = context.getName();
        AtomicBoolean approved = new AtomicBoolean(true);
        AtomicInteger responseCount = new AtomicInteger(1);
        Set<ClusterNode> nodes = context.getMessagingNodes();
        CountDownLatch countdownLatch = new CountDownLatch(nodes.size() - 1);
        NodeContext message = newBuilder().setNodeName(thisNodeName).setContext(context.getName()).build();
        nodes.stream().filter(this::isNotThisNode).forEach(
                node -> sender.send(message,
                                    node,
                                    new ConfirmationTarget(node::getName, approved, responseCount, countdownLatch))
        );
        countdownLatch.await(waitMilliseconds, MILLISECONDS);
        if (coordinatorFound.get()) return true;
        if (approved.get() && hasQuorumToChange(context.getMessagingNodes().size(), responseCount.get())) {
            logger.info("Become coordinator for context {} ", contextName);
            eventPublisher.publishEvent(new BecomeCoordinator(thisNodeName, contextName, false));
            return true;
        }
        return false;
    }

    private boolean hasQuorumToChange(int nodesInCluster, int responseCount) {
        return responseCount > 1 && responseCount >= nodesInCluster / 2f + 0.01;
    }

    private boolean isNotThisNode(ClusterNode node) {
        return !node.getName().equals(thisNodeName);
    }
}
