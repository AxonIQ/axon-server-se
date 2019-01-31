package io.axoniq.axonserver.cluster.election;

import io.axoniq.axonserver.cluster.RaftPeer;
import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import io.axoniq.axonserver.grpc.cluster.RequestVoteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static java.lang.String.format;

/**
 * @author Sara Pellegrini
 * @since 4.1
 */
public class DefaultElection implements Election {

    private final Logger logger = LoggerFactory.getLogger(DefaultElection.class);

    private final RequestVoteRequest requestPrototype;
    private final BiConsumer<Long, String> termUpdateHandler;
    private final ElectionStore electionStore;
    private final Collection<RaftPeer> otherNodes;
    private final VoteStrategy voteStrategy;
    private final Mono<Result> result;
    private final AtomicReference<MonoSink<Result>> sink = new AtomicReference<>();

    public DefaultElection(RequestVoteRequest requestPrototype,
                           BiConsumer<Long, String> termUpdateHandler,
                           ElectionStore electionStore,
                           Collection<RaftPeer> otherNodes) {
        this(requestPrototype,
             termUpdateHandler,
             electionStore,
             otherNodes,
             new MajorityStrategy(() -> otherNodes.size() + 1));
    }

    public DefaultElection(RequestVoteRequest requestPrototype,
                           BiConsumer<Long, String> termUpdateHandler,
                           ElectionStore electionStore,
                           Collection<RaftPeer> otherNodes, VoteStrategy voteStrategy) {
        this.requestPrototype = requestPrototype;
        this.termUpdateHandler = termUpdateHandler;
        this.electionStore = electionStore;
        this.otherNodes = otherNodes;
        this.voteStrategy = voteStrategy;
        this.result = Mono.create(sink::set);
    }

    public Mono<Result> result(){
        String cause = format("%s is starting a new election, so increases its term from %s to %s", me(), currentTerm(), electionTerm());
        updateCurrentTerm(electionTerm(), cause);
        electionStore.markVotedFor(me());
        logger.info("{}: Starting election from {} in term {}", groupId(), me(), currentTerm());
        voteStrategy.registerVoteReceived(me(), true);
        if (otherNodes.isEmpty()) {
            notifyElectionWon();
        } else {
            otherNodes.forEach(node -> requestVote(request(), node));
        }
        return result;
    }

    private void requestVote(RequestVoteRequest request, RaftPeer node) {
        node.requestVote(request).thenAccept(this::onVoteResponse);
    }

    private synchronized void onVoteResponse(RequestVoteResponse response){
        String voter = response.getResponseHeader().getNodeId();
        logger.trace("{} - currentTerm {} VoteResponse {}", voter, currentTerm(), response);
        if (response.getTerm() > currentTerm()) {
            String message = format("%s received RequestVoteResponse with greater term (%s > %s) from %s",
                                    me(), response.getTerm(), currentTerm(), voter);
            updateCurrentTerm(response.getTerm(), message);
            sink.get().success(result(false, message));
            return;
        }

        //The candidate can receive a response with lower term if the voter is receiving regular heartbeat from a leader.
        //In this case, the voter recognizes any request of vote as disruptive, refuses the vote and does't update its term.
        if (response.getTerm() < currentTerm()) {
            return;
        }
        voteStrategy.registerVoteReceived(voter, response.getVoteGranted());
        if (voteStrategy.isWon()) {
            notifyElectionWon();
        }
    }

    private RequestVoteRequest request(){
        return RequestVoteRequest.newBuilder(requestPrototype).setRequestId(UUID.randomUUID().toString()).build();
    }

    private String me(){
        return requestPrototype.getCandidateId();
    }

    private String groupId(){
        return requestPrototype.getGroupId();
    }

    private long electionTerm(){
        return requestPrototype.getTerm();
    }

    private long currentTerm(){
        return electionStore.currentTerm();
    }

    private void updateCurrentTerm(long term, String cause){
        termUpdateHandler.accept(term, cause);
    }

    private void notifyElectionWon(){
        String msg = format("%s: Election for term %s won by %s (%s)", groupId(), electionTerm(), me(), voteStrategy);
        sink.get().success(result(true, msg));
    }


    private Result result(boolean won, String cause){
        return new Result() {
            @Override
            public boolean won() {
                return won;
            }

            @Override
            public String cause() {
                return cause;
            }
        };
    }
}
