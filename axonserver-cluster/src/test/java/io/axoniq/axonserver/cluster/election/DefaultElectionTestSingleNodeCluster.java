package io.axoniq.axonserver.cluster.election;

import io.axoniq.axonserver.grpc.cluster.RequestVoteRequest;
import org.junit.*;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static org.junit.Assert.*;

/**
 * @author Sara Pellegrini
 * @since
 */
public class DefaultElectionTestSingleNodeCluster {

    private DefaultElection election;
    private ElectionStore electionStore;

    @Before
    public void setUp() throws Exception {
        electionStore = new InMemoryElectionStore();
        electionStore.updateCurrentTerm(100);
        RequestVoteRequest prototype = RequestVoteRequest.newBuilder()
                                                         .setTerm(101)
                                                         .setCandidateId("Node0")
                                                         .setLastLogTerm(100)
                                                         .setLastLogIndex(1000)
                                                         .setGroupId("MyGroupId")
                                                         .build();
        BiConsumer<Long, String> updateTerm = (newTerm, cause) -> electionStore.updateCurrentTerm(newTerm);
        election = new DefaultElection(prototype, updateTerm, electionStore, Collections.emptyList(),
                                       () -> 1,
                                       false);
    }

    @Test
    public void testElectionWon() {
        AtomicBoolean won = new AtomicBoolean(false);
        election.result().subscribe(result -> won.set(result.won()));
        assertTrue(won.get());
    }

}