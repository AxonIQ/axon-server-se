package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.cluster.jpa.ReplicationGroupMember;
import io.axoniq.axonserver.cluster.jpa.ReplicationGroupMemberRepository;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.logging.AuditLog;
import io.swagger.annotations.Api;
import org.slf4j.Logger;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.security.Principal;
import java.util.Set;

/**
 * @author Marc Gathier
 */
@RestController
@Api(tags = "internal", hidden = true)
@RequestMapping("internal/raft")
public class RaftGroupInfoService {

    private static final Logger auditLog = AuditLog.getLogger();

    private final ReplicationGroupMemberRepository raftGroupNodeRepository;
    private final MessagingPlatformConfiguration messagingPlatformConfiguration;


    public RaftGroupInfoService(ReplicationGroupMemberRepository raftGroupNodeRepository,
                                MessagingPlatformConfiguration messagingPlatformConfiguration) {
        this.raftGroupNodeRepository = raftGroupNodeRepository;
        this.messagingPlatformConfiguration = messagingPlatformConfiguration;
    }

    @GetMapping("groups")
    public Set<ReplicationGroupMember> status(Principal principal) {
        auditLog.info("[{}] Request to list replication groups on this node.",
                      AuditLog.username(principal));

        return raftGroupNodeRepository.findByNodeName(messagingPlatformConfiguration.getName());
    }

    @GetMapping("members/{group}")
    public Set<ReplicationGroupMember> members(@PathVariable("group") String group, Principal principal) {
        auditLog.info("[{}] Request to list replication group members for {} on this node.",
                      AuditLog.username(principal), group);
        return raftGroupNodeRepository.findByGroupId(group);
    }

}
