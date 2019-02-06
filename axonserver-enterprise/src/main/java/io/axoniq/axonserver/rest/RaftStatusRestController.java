package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.enterprise.cluster.GrpcRaftController;
import io.axoniq.axonserver.serializer.Media;
import io.axoniq.axonserver.serializer.Printable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Sara Pellegrini
 * @since 4.1
 */
@RestController
@RequestMapping("v1/raft")
public class RaftStatusRestController {

    private final GrpcRaftController grpcRaftController;

    public RaftStatusRestController(GrpcRaftController grpcRaftController) {
        this.grpcRaftController = grpcRaftController;
    }

    @GetMapping("status")
    public List<RaftContext> status(){
        Iterable<String> myContexts = grpcRaftController.getMyContexts();
        List<RaftContext> raftContexts = new LinkedList<>();
        for (String context : myContexts) {
            raftContexts.add(new RaftContext(grpcRaftController.getRaftGroup(context)));
        }
        return raftContexts;
    }

    private static class RaftContext implements Printable {

        private final RaftGroup raftGroup;

        private RaftContext(RaftGroup raftGroup) {
            this.raftGroup = raftGroup;
        }

        @Override
        public void printOn(Media media) {
            media.with("context", raftGroup.raftConfiguration().groupId());
            media.with("commitIndex", raftGroup.logEntryProcessor().commitIndex());
            media.with("commitTerm", raftGroup.logEntryProcessor().commitTerm());
            media.with("lastAppliedIndex", raftGroup.logEntryProcessor().lastAppliedIndex());
            media.with("lastApplierTerm", raftGroup.logEntryProcessor().lastAppliedTerm());
            media.with("lastLongIndex", raftGroup.localLogEntryStore().lastLogIndex());
            media.with("lastLongTerm", raftGroup.localLogEntryStore().lastLog().getTerm());
            media.with("minElectionTimeout", raftGroup.raftConfiguration().minElectionTimeout());
            media.with("maxElectionTimeout", raftGroup.raftConfiguration().maxElectionTimeout());
            media.with("heartbeatTimeout", raftGroup.raftConfiguration().heartbeatTimeout());
            media.with("maxEntriesPerBatch", raftGroup.raftConfiguration().maxEntriesPerBatch());
            media.with("maxReplicationRound", raftGroup.raftConfiguration().maxReplicationRound());
            media.with("flowBuffer", raftGroup.raftConfiguration().flowBuffer());
        }
    }
}
