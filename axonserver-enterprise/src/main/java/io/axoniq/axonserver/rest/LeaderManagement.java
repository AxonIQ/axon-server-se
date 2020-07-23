package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.enterprise.replication.GrpcRaftController;
import io.axoniq.axonserver.enterprise.replication.group.LocalRaftGroupService;
import io.axoniq.axonserver.logging.AuditLog;
import org.slf4j.Logger;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.security.Principal;

/**
 * Rest APIs for Leader management. Only enabled when active profiles contains internal.
 *
 * @author Marc Gathier
 * @since 4.1
 */
@RestController
@CrossOrigin
@Profile("internal")
@RequestMapping("internal")
public class LeaderManagement {

    private static final Logger auditLog = AuditLog.getLogger();

    private final LocalRaftGroupService localRaftGroupService;
    private final GrpcRaftController grpcRaftController;

    public LeaderManagement(LocalRaftGroupService localRaftGroupService,
                            GrpcRaftController grpcRaftController) {
        this.localRaftGroupService = localRaftGroupService;
        this.grpcRaftController = grpcRaftController;
    }

    /**
     * Forces the current leader for the specified context to step down.
     *
     * @param name the context
     */
    @GetMapping(path = "context/{name}/stepdown")
    public void stepdown(@PathVariable("name") String name, Principal principal) {
        auditLog.info("[{}] Request to step down in {}.", AuditLog.username(principal), name);
        localRaftGroupService.stepDown(name);
    }

    @GetMapping(path = "context/{name}/fatal")
    public void fatal(@PathVariable("name") String name, Principal principal) {
        auditLog.info("[{}] Request to move to fatal state in {}.", AuditLog.username(principal), name);
        grpcRaftController.getRaftNode(name).changeToFatal();
    }

    @GetMapping(path = "context/{name}/follower")
    public void follower(@PathVariable("name") String name, Principal principal) {
        auditLog.info("[{}] Request to move to follower state in {}.", AuditLog.username(principal), name);
        grpcRaftController.getRaftNode(name).changeToFollower();
    }
}
