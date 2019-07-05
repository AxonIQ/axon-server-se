package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.KeepNames;
import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.enterprise.cluster.GrpcRaftController;
import io.axoniq.axonserver.grpc.cluster.Node;
import io.axoniq.axonserver.serializer.Media;
import io.axoniq.axonserver.serializer.Printable;
import io.swagger.annotations.Api;
import org.jetbrains.annotations.NotNull;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Sara Pellegrini
 * @since 4.1
 */
@RestController
@RequestMapping("internal/raft")
@Api(tags = "internal", hidden = true)
public class RaftStatusRestController {

    private final GrpcRaftController grpcRaftController;

    public RaftStatusRestController(GrpcRaftController grpcRaftController) {
        this.grpcRaftController = grpcRaftController;
    }

    @GetMapping("status")
    public List<RaftContext> status() {
        Iterable<String> myContexts = grpcRaftController.raftGroups();
        List<RaftContext> raftContexts = new LinkedList<>();
        for (String context : myContexts) {
            raftContexts.add(new RaftContext(grpcRaftController.getRaftGroup(context)));
        }
        return raftContexts;
    }

    @KeepNames
    private static class RaftContext implements Printable {

        private final RaftGroup raftGroup;

        private RaftContext(RaftGroup raftGroup) {
            this.raftGroup = raftGroup;
        }

        @Override
        public void printOn(Media media) {
            media.with("nodeId", raftGroup.localNode().nodeId());
            media.with("context", raftGroup.raftConfiguration().groupId());
            media.with("commitIndex", raftGroup.logEntryProcessor().commitIndex());
            media.with("commitTerm", raftGroup.logEntryProcessor().commitTerm());
            media.with("lastAppliedIndex", raftGroup.logEntryProcessor().lastAppliedIndex());
            media.with("lastAppliedTerm", raftGroup.logEntryProcessor().lastAppliedTerm());
            media.with("lastLogIndex", raftGroup.localLogEntryStore().lastLogIndex());
            media.with("lastLogTerm", raftGroup.localLogEntryStore().lastLog().getTerm());
            media.with("firstLogIndex", raftGroup.localLogEntryStore().firstLogIndex());
            media.with("firstLogTerm", raftGroup.localLogEntryStore().firstLog().getTerm());
            media.with("minElectionTimeout", raftGroup.raftConfiguration().minElectionTimeout());
            media.with("maxElectionTimeout", raftGroup.raftConfiguration().maxElectionTimeout());
            media.with("heartbeatTimeout", raftGroup.raftConfiguration().heartbeatTimeout());
            media.with("maxEntriesPerBatch", raftGroup.raftConfiguration().maxEntriesPerBatch());
            media.with("maxReplicationRound", raftGroup.raftConfiguration().maxReplicationRound());
            media.with("flowBuffer", raftGroup.raftConfiguration().flowBuffer());
            media.with("currentTerm", raftGroup.localElectionStore().currentTerm());
            media.with("votedFor", raftGroup.localElectionStore().votedFor());
            media.with("leaderId", raftGroup.localNode().getLeader());
            media.with("leaderName", raftGroup.localNode().getLeaderName());
            media.with("isLeader", raftGroup.localNode().isLeader());
            media.with("configuration", new Nodes(raftGroup.raftConfiguration().groupMembers()));
        }
    }

    private static class Nodes implements Iterable<Printable> {

        private final Iterable<Node> nodes;

        private Nodes(Iterable<Node> nodes) {
            this.nodes = nodes;
        }

        @NotNull
        @Override
        public Iterator<Printable> iterator() {
            Iterator<Node> iterator = nodes.iterator();
            return new Iterator<Printable>() {
                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public Printable next() {
                    Node next = iterator.next();
                    return media -> media.with("id", next.getNodeId())
                                         .with("port", next.getPort())
                                         .with("host", next.getHost());
                }
            };
        }
    }
}
