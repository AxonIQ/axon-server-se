package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.enterprise.context.ContextNameValidation;
import io.axoniq.axonserver.enterprise.jpa.AdminReplicationGroupMember;
import io.axoniq.axonserver.enterprise.jpa.AdminReplicationGroupRepository;
import io.axoniq.axonserver.enterprise.replication.RaftLeaderProvider;
import io.axoniq.axonserver.enterprise.replication.admin.RaftConfigServiceFactory;
import io.axoniq.axonserver.enterprise.taskscheduler.task.PrepareDeleteNodeFromContextTask;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupMember;
import io.axoniq.axonserver.logging.AuditLog;
import io.axoniq.axonserver.rest.json.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.security.Principal;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.validation.Valid;

/**
 * @author Marc Gathier
 */
@RestController
@CrossOrigin
@RequestMapping("/v1")
public class ReplicationGroupRestController {

    private static final Logger auditLog = AuditLog.getLogger();

    private final RaftConfigServiceFactory raftServiceFactory;
    private final RaftLeaderProvider raftLeaderProvider;
    private final AdminReplicationGroupRepository replicationGroupRepository;
    private final PrepareDeleteNodeFromContextTask deleteNodeFromContextJob;
    private final Predicate<String> contextNameValidation = new ContextNameValidation();
    private final Logger logger = LoggerFactory.getLogger(ReplicationGroupRestController.class);

    public ReplicationGroupRestController(RaftConfigServiceFactory raftServiceFactory,
                                          RaftLeaderProvider raftLeaderProvider,
                                          AdminReplicationGroupRepository replicationGroupRepository,
                                          PrepareDeleteNodeFromContextTask deleteNodeFromContextJob) {
        this.raftServiceFactory = raftServiceFactory;
        this.raftLeaderProvider = raftLeaderProvider;
        this.replicationGroupRepository = replicationGroupRepository;
        this.deleteNodeFromContextJob = deleteNodeFromContextJob;
    }

    @GetMapping(path = "public/replicationgroups")
    public List<ReplicationGroupJSON> getReplicationGroups(Principal principal) {
        auditLog.info("[{}] Request to list replication groups.", AuditLog.username(principal));
        return replicationGroupRepository.findAll().stream().map(replicationGroup -> {
            ReplicationGroupJSON json = new ReplicationGroupJSON(replicationGroup.getName());
            json.setChangePending(replicationGroup.isChangePending());
            if (replicationGroup.getPendingSince() != null) {
                json.setPendingSince(replicationGroup.getPendingSince().getTime());
            }
            json.setLeader(raftLeaderProvider.getLeader(replicationGroup.getName()));
            json.setRoles(replicationGroup.getMembers().stream().map(NodeAndRole::new).sorted()
                                          .collect(Collectors.toList()));
            if (replicationGroup.getMembers().stream().anyMatch(AdminReplicationGroupMember::isPendingDelete)) {
                json.setChangePending(true);
            }
            json.setContexts(replicationGroup.getContexts().stream().map(ContextJSON::new)
                                             .collect(Collectors.toList()));

            return json;
        }).sorted(Comparator.comparing(ReplicationGroupJSON::getName)).collect(Collectors.toList());
    }

    @DeleteMapping(path = "replicationgroups/{name}")
    public ResponseEntity<RestResponse> deleteContext(
            @PathVariable("name") String name,
            @RequestParam(value = "preserveEventStore", required = false, defaultValue = "false") boolean preserveEventStore,
            Principal principal) {
        auditLog.info("[{}] Request to delete replication group {} preserve event stores {}.",
                      AuditLog.username(principal), name, preserveEventStore);
        logger.info("Delete request received for replicationgroup: {}", name);
        try {
            if (name.startsWith("_")) {
                return new RestResponse(false, String.format(
                        "Cannot delete internal context %s",
                        name))
                        .asResponseEntity(ErrorCode.CANNOT_DELETE_INTERNAL_CONTEXT);
            }
            raftServiceFactory.getRaftConfigService().deleteReplicationGroup(name, preserveEventStore);
            return ResponseEntity.accepted()
                                 .body(new RestResponse(true,
                                                        "Accepted delete request"));
        } catch (Exception ex) {
            return new RestResponse(false, ex.getMessage()).asResponseEntity(ErrorCode.fromException(ex));
        }
    }

    @PostMapping(path = "replicationgroups/{replicationgroup}/{node}")
    public ResponseEntity<RestResponse> updateNodeRoles(
            @PathVariable("replicationgroup") String name,
            @PathVariable("node") String node,
            @RequestParam(value = "role", required = false, defaultValue = "PRIMARY") String role,
            Principal principal) {
        auditLog.info("[{}] Request to add node {} to replication group {} with role {}.",
                      AuditLog.username(principal), node, name, role);
        logger.info("Add node request received for node: {} - and context: {}", node, name);
        try {
            raftServiceFactory.getRaftConfigService().addNodeToReplicationGroup(name,
                                                                                node,
                                                                                role == null ? Role.PRIMARY : Role
                                                                                        .valueOf(role));
            return ResponseEntity.accepted()
                                 .body(new RestResponse(true,
                                                        "Started to add node to replication group. This may take some time depending on the number of events already in the contexts"));
        } catch (Exception ex) {
            return new RestResponse(false, ex.getMessage()).asResponseEntity(ErrorCode.fromException(ex));
        }
    }

    @DeleteMapping(path = "replicationgroups/{replicationgroup}/{node}")
    public ResponseEntity<RestResponse> deleteNodeFromContext(
            @PathVariable("replicationgroup") String name,
            @PathVariable("node") String node,
            @RequestParam(name = "preserveEventStore", required = false, defaultValue = "false") boolean preserveEventStore,
            Principal principal) {
        auditLog.info("[{}] Request to delete node {} from replication group {} preserve events stores  {}.",
                      AuditLog.username(principal), node, name, preserveEventStore);
        logger.info("Delete node request received for node: {} - and replication group: {} {}",
                    node,
                    name,
                    preserveEventStore ? "- Keeping event store" : "");
        try {
            deleteNodeFromContextJob.prepareDeleteNodeFromContext(name, node, preserveEventStore);
            return ResponseEntity.accepted()
                                 .body(new RestResponse(true,
                                                        "Accepted delete request"));
        } catch (Exception ex) {
            return new RestResponse(false, ex.getMessage()).asResponseEntity(ErrorCode.fromException(ex));
        }
    }

    @PostMapping(path = "replicationgroups")
    public ResponseEntity<RestResponse> addReplicationGroup(
            @RequestBody @Valid ReplicationGroupJSON replicationGroupJSON, Principal principal) {
        auditLog.info("[{}] Request to create replication group {} with nodes {}.",
                      AuditLog.username(principal), replicationGroupJSON.getName(), replicationGroupJSON.roles());
        logger.info("Add replication group request received for replication group: {}", replicationGroupJSON.getName());

        // Backwards compatibility, if nodes is specified and no roles provide, add nodes as primary
        if (!replicationGroupJSON.hasRoles()) {
            return new RestResponse(false, "Add at least one node for the application group")
                    .asResponseEntity(ErrorCode.CONTEXT_CREATION_NOT_ALLOWED);
        }

        if (!contextNameValidation.test(replicationGroupJSON.getName())) {
            return new RestResponse(false, "Invalid replication group name")
                    .asResponseEntity(ErrorCode.INVALID_CONTEXT_NAME);
        }

        try {
            raftServiceFactory.getRaftConfigService()
                              .createReplicationGroup(replicationGroupJSON.getName(),
                                                      replicationGroupJSON.getRoles()
                                                                          .stream()
                                                                          .map(n -> ReplicationGroupMember
                                                                                  .newBuilder()
                                                                                  .setNodeName(n.getNode())
                                                                                  .setRole(n.getRole())
                                                                                  .build()
                                                                          )
                                                                          .collect(
                                                                                  Collectors
                                                                                          .toList()));
            return ResponseEntity.ok(new RestResponse(true, null));
        } catch (Exception ex) {
            return new RestResponse(false, ex.getMessage()).asResponseEntity(ErrorCode.fromException(ex));
        }
    }

    /**
     * Returns a set of roles that can be assigned to nodes in a replication group.
     *
     * @return set of role names
     */
    @GetMapping(path = "replicationgroups/roles")
    public Set<String> roles(Principal principal) {
        auditLog.info("[{}] Request to list replication group roles.",
                      AuditLog.username(principal));
        return Arrays.stream(Role.values())
                     .filter(r -> !Role.UNRECOGNIZED.equals(r))
                     .map(Enum::name)
                     .collect(Collectors.toSet());
    }
}
