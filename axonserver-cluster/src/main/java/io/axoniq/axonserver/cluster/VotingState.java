package io.axoniq.axonserver.cluster;

import io.axoniq.axonserver.cluster.configuration.CandidateConfiguration;
import io.axoniq.axonserver.cluster.configuration.ClusterConfiguration;
import io.axoniq.axonserver.cluster.scheduler.Scheduler;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesRequest;
import io.axoniq.axonserver.grpc.cluster.AppendEntriesResponse;
import io.axoniq.axonserver.grpc.cluster.ConfigChangeResult;
import io.axoniq.axonserver.grpc.cluster.Node;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.String.format;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Generic base for states that perform a (pre)election ({@link PreVoteState} and {@link CandidateState}).
 *
 * @author Marc Gathier
 * @since 4.2
 */
public abstract class VotingState extends AbstractMembershipState {

    private final Logger logger;
    protected final AtomicReference<Scheduler> scheduler = new AtomicReference<>();
    private final ClusterConfiguration clusterConfiguration = new CandidateConfiguration();

    VotingState(Builder builder, Logger logger) {
        super(builder);
        this.logger = logger;
    }

    @Override
    public void start() {
        scheduler.set(schedulerFactory().get());
        startElection();
    }


    @Override
    public void stop() {
        if (scheduler.get() != null) {
            scheduler.getAndSet(null).shutdownNow();
        }
    }

    @Override
    public AppendEntriesResponse appendEntries(AppendEntriesRequest request) {
        if (request.getTerm() >= currentTerm()) {
            logger.info("{} in term {}: Received term {} which is greater or equals than mine. Moving to Follower...",
                        groupId(),
                        currentTerm(),
                        request.getTerm());
            String message = format("%s received AppendEntriesRequest with greater or equals term (%s >= %s) from %s",
                                    me(),
                                    request.getTerm(),
                                    currentTerm(),
                                    request.getLeaderId());
            return handleAsFollower(follower -> follower.appendEntries(request), message);
        }
        logger.info("{} in term {}: Received term {} is smaller than mine. Rejecting the request.",
                    groupId(),
                    currentTerm(),
                    request.getTerm());
        return responseFactory().appendEntriesFailure(request.getRequestId(),
                                                      "Request rejected because I'm a candidate");
    }

    @Override
    public CompletableFuture<ConfigChangeResult> addServer(Node node) {
        return clusterConfiguration.addServer(node);
    }

    @Override
    public CompletableFuture<ConfigChangeResult> removeServer(String nodeId) {
        return clusterConfiguration.removeServer(nodeId);
    }

    void resetElectionTimeout() {
        int timeout = random(minElectionTimeout(), maxElectionTimeout() + 1);
        ofNullable(scheduler.get()).ifPresent(s -> s.schedule(this::startElection, timeout, MILLISECONDS));
    }

    protected abstract void startElection();
}
