package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.cluster.RaftGroup;
import io.axoniq.axonserver.cluster.RaftNode;
import io.axoniq.axonserver.enterprise.replication.GrpcRaftController;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.logging.AuditLog;
import io.swagger.annotations.Api;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.security.Principal;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Rest APIs for Raft management.
 *
 * @author Sara Pellegrini
 * @since 4.1
 */
@RestController
@CrossOrigin
@Profile("internal")
@Api(tags = "internal", hidden = true)
@RequestMapping("internal/raft")
public class RaftManagementRestController {

    private static final Logger auditLog = AuditLog.getLogger();

    private static final Logger logger = LoggerFactory.getLogger(RaftManagementRestController.class);

    private final GrpcRaftController grpcRaftController;

    public RaftManagementRestController(
            GrpcRaftController grpcRaftController) {
        this.grpcRaftController = grpcRaftController;
    }

    /**
     * Cleans the RAFT log for given {@code context}.
     *
     * @param context for which the will be cleaned
     * @param seconds log entries older than this amount of seconds will not be deleted
     */
    @PostMapping(path = "context/{context}/cleanLogEntries/{seconds}")
    public void cleanLogOlderThen(@PathVariable("context") String context, @PathVariable("seconds") long seconds,
                                  Principal principal) {
        auditLog.info("[{}] Request to clean replication group logs for {} older than {} seconds.",
                      AuditLog.username(principal), context, seconds);
        RaftNode raftNode = localNode(context);
        if (raftNode == null) {
            logger.info("Cannot perform log compaction: context {} not found.", context);
            return;
        }
        logger.info("Cleaning RAFT log for context {} older than {} seconds.", context, seconds);
        raftNode.forceLogCleaning(seconds, SECONDS);
        logger.info("RAFT log cleared for context {} older than {} seconds.", context, seconds);
    }

    /**
     * Starts the node for given {@code context}.
     *
     * @param context the context
     */
    @PostMapping(path = "context/{context}/start")
    public void startContext(@PathVariable("context") String context, Principal principal) {
        auditLog.info("[{}] Request to start replication group {} on this node.",
                      AuditLog.username(principal), context);
        logger.info("Starting RAFT node for context {}.", context);
        localNode(context).start();
        logger.info("RAFT node started for context {}.", context);
    }

    /**
     * Stops the node for given {@code context}.
     *
     * @param context the context
     */
    @PostMapping(path = "context/{context}/stop")
    public void stopContext(@PathVariable("context") String context, Principal principal) {
        auditLog.info("[{}] Request to stop replication group {} on this node.",
                      AuditLog.username(principal), context);
        logger.info("Stopping RAFT node for context {}.", context);
        localNode(context).stop();
        logger.info("RAFT node stopped for context {}.", context);
    }

    private RaftNode localNode(String context) {
        RaftGroup raftGroup = grpcRaftController.getRaftGroup(context);
        if (raftGroup == null) {
            throw new MessagingPlatformException(ErrorCode.CONTEXT_NOT_FOUND, context + ": not found");
        }
        return raftGroup.localNode();
    }
}
