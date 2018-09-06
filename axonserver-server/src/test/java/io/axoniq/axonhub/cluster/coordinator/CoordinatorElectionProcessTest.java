package io.axoniq.axonhub.cluster.coordinator;

import io.axoniq.axonhub.ClusterEvents;
import io.axoniq.axonhub.Confirmation;
import io.axoniq.axonhub.cluster.jpa.ClusterNode;
import io.axoniq.axonhub.context.jpa.Context;
import io.axoniq.axonhub.internal.grpc.NodeContext;
import io.axoniq.axonhub.spring.FakeApplicationEventPublisher;
import io.grpc.stub.StreamObserver;
import org.junit.*;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Created by Sara Pellegrini on 27/08/2018.
 * sara.pellegrini@gmail.com
 */
public class CoordinatorElectionProcessTest {

    private Set<ClusterNode> nodes = new HashSet<ClusterNode>(){{
        this.add(new ClusterNode("nodeA", "localhost", "localhost", 1,2,3));
        this.add(new ClusterNode("nodeB", "localhost", "localhost", 1,2,3));
        this.add(new ClusterNode("nodeC", "localhost", "localhost", 1,2,3));
        this.add(new ClusterNode("nodeD", "localhost", "localhost", 1,2,3));
    }};

    private Context context;

    private Sender<NodeContext, ClusterNode, StreamObserver<Confirmation>> sender;

    private FakeApplicationEventPublisher eventPublisher;
    private AtomicBoolean elected;

    @Before
    public void setUp() throws Exception {
        context = mock(Context.class);
        when(context.getMessagingNodes()).thenReturn(nodes);
        when(context.getName()).thenReturn("defaultContext");
        elected = new AtomicBoolean();
        eventPublisher = new FakeApplicationEventPublisher();
        eventPublisher.add(event -> {
            if (event instanceof ClusterEvents.BecomeCoordinator) elected.set(true);
        });
    }

    @Test
    public void testCandidateAccepted() {
        sender = (message, node, callback) -> callback.onNext(Confirmation.newBuilder().setSuccess(true).build());
        CoordinatorElectionProcess electionProcess = new CoordinatorElectionProcess("nodeA", 100L, eventPublisher, sender);
        electionProcess.startElection(context, elected::get);
        assertTrue(elected.get());
    }

    @Test
    public void testNoQuorum() throws InterruptedException {
        sender = (message, node, callback) -> {
            if (node.getName().equals("nodeB")) callback.onNext(Confirmation.newBuilder().setSuccess(true).build());
            else callback.onError(new RuntimeException("Node B unavailable"));
        };
        CoordinatorElectionProcess electionProcess = new CoordinatorElectionProcess("nodeA", 100L, eventPublisher, sender);
        electionProcess.electionRound(context, elected::get);
        assertFalse(elected.get());
    }

    @Test
    public void testCandidateRejected() throws InterruptedException {
        sender = (message, node, callback) -> {
            if (node.getName().equals("nodeB")) callback.onNext(Confirmation.newBuilder().setSuccess(false).build());
            else callback.onNext(Confirmation.newBuilder().setSuccess(true).build());
        };
        CoordinatorElectionProcess electionProcess = new CoordinatorElectionProcess("nodeA", 100L, eventPublisher, sender);
        electionProcess.electionRound(context, elected::get);
        assertFalse(elected.get());
    }
}