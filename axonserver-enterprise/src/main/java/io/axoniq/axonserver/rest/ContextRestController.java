package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.enterprise.context.ContextNameValidation;
import io.axoniq.axonserver.enterprise.jpa.AdminContext;
import io.axoniq.axonserver.enterprise.jpa.AdminContextRepository;
import io.axoniq.axonserver.enterprise.jpa.AdminReplicationGroup;
import io.axoniq.axonserver.enterprise.jpa.AdminReplicationGroupMember;
import io.axoniq.axonserver.enterprise.replication.ContextLeaderProvider;
import io.axoniq.axonserver.enterprise.replication.RaftLeaderProvider;
import io.axoniq.axonserver.enterprise.replication.admin.AdminReplicationGroupController;
import io.axoniq.axonserver.enterprise.replication.admin.RaftConfigServiceFactory;
import io.axoniq.axonserver.enterprise.storage.ContextPropertyDefinition;
import io.axoniq.axonserver.enterprise.topology.ClusterTopology;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.internal.ReplicationGroupMember;
import io.axoniq.axonserver.logging.AuditLog;
import io.axoniq.axonserver.rest.json.RestResponse;
import io.axoniq.axonserver.topology.Topology;
import io.axoniq.axonserver.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.validation.Valid;

import static io.axoniq.axonserver.RaftAdminGroup.isAdmin;

/**
 * @author Marc Gathier
 */
@RestController
@CrossOrigin
@RequestMapping("/v1")
public class ContextRestController {

    private static final Logger auditLog = AuditLog.getLogger();

    private final RaftConfigServiceFactory raftServiceFactory;
    private final ContextLeaderProvider contextLeaderProvider;
    private final RaftLeaderProvider raftLeaderProvider;
    private final AdminContextRepository adminContextRepository;
    private final AdminReplicationGroupController adminReplicationGroupController;
    private final ClusterTopology clusterTopology;
    private final Predicate<String> contextNameValidation = new ContextNameValidation();
    private final Logger logger = LoggerFactory.getLogger(ContextRestController.class);

    public ContextRestController(RaftConfigServiceFactory raftServiceFactory,
                                 ContextLeaderProvider contextLeaderProvider,
                                 RaftLeaderProvider raftLeaderProvider,
                                 AdminContextRepository adminContextRepository,
                                 AdminReplicationGroupController adminReplicationGroupController,
                                 ClusterTopology clusterTopology) {
        this.raftServiceFactory = raftServiceFactory;
        this.contextLeaderProvider = contextLeaderProvider;
        this.raftLeaderProvider = raftLeaderProvider;
        this.adminContextRepository = adminContextRepository;
        this.adminReplicationGroupController = adminReplicationGroupController;
        this.clusterTopology = clusterTopology;
    }

    @GetMapping(path = "public/context")
    @Transactional
    public List<ContextJSON> getContexts(Principal principal) {
        auditLog.info("[{}] Request to list contexts.", AuditLog.username(principal));
        return adminContextRepository.findAll().stream().map(context -> {
            ContextJSON json = new ContextJSON(context.getName());
            json.setChangePending(context.getReplicationGroup().isChangePending());
            if (context.getPendingSince() != null) {
                json.setPendingSince(context.getPendingSince().getTime());
            }
            json.setLeader(contextLeaderProvider.getLeader(context.getName()));
            json.setReplicationGroup(context.getReplicationGroup().getName());
            json.setRoles(context.getReplicationGroup().getMembers().stream().map(NodeAndRole::new)
                                 .sorted()
                                 .collect(Collectors.toList()));
            if (context.getReplicationGroup().getMembers().stream()
                       .anyMatch(AdminReplicationGroupMember::isPendingDelete)) {
                json.setChangePending(true);
            }
            json.setMetaData(context.getMetaDataMap());
            return json;
        }).sorted(Comparator.comparing(ContextJSON::getContext)).collect(Collectors.toList());
    }

    /**
     * Get the names of the context that should be displayed in the Axon Dashboard. Names depend on whether the
     * connected node is an
     * admin node (all known contexts) or not (context defined on this node).
     *
     * @param includeAdmin include admin context in result (default false)
     * @return names of contexts
     */
    @GetMapping(path = "public/visiblecontexts")
    public Iterable<String> visibleContexts(
            @RequestParam(name = "includeAdmin", required = false, defaultValue = "false") boolean includeAdmin,
            Principal principal) {
        auditLog.info("[{}] Request to list contexts.", AuditLog.username(principal));
        if (clusterTopology.isAdminNode()) {
            return adminContextRepository.findAll().stream()
                                         .map(AdminContext::getName)
                                         .filter(name -> includeAdmin || !isAdmin(name))
                                         .sorted()
                                         .collect(Collectors.toList());
        } else {
            return StreamSupport.stream(clusterTopology.getMyContextNames().spliterator(), false)
                                .filter(name -> includeAdmin || !isAdmin(name))
                                .sorted()
                                .collect(Collectors.toList());
        }
    }

    @DeleteMapping(path = "context/{name}")
    public ResponseEntity<RestResponse> deleteContext(@PathVariable("name") String name,
                                                      @RequestParam(name = "preserveEventStore", required = false, defaultValue = "false") boolean preserveEventStore,
                                                      Principal principal
    ) {
        auditLog.info("[{}] Request to delete context {} preserve event store {}.",
                      AuditLog.username(principal),
                      name,
                      preserveEventStore);
        try {
            if (name.startsWith("_")) {
                return new RestResponse(false, String.format(
                        "Cannot delete internal context %s",
                        name))
                        .asResponseEntity(ErrorCode.CANNOT_DELETE_INTERNAL_CONTEXT);
            }
            raftServiceFactory.getRaftConfigService().deleteContext(name, preserveEventStore);
            return ResponseEntity.accepted()
                                 .body(new RestResponse(true,
                                                        "Accepted delete request"));
        } catch (Exception ex) {
            return new RestResponse(false, ex.getMessage()).asResponseEntity(ErrorCode.fromException(ex));
        }
    }

    @PostMapping(path = "context")
    public ResponseEntity<RestResponse> addContext(@RequestBody @Valid ContextJSON contextJson, Principal principal) {
        auditLog.info("[{}] Request to add context {}.", AuditLog.username(principal), contextJson.getContext());

        if (!contextNameValidation.test(contextJson.getContext())) {
            return new RestResponse(false, "Invalid context name").asResponseEntity(ErrorCode.INVALID_CONTEXT_NAME);
        }

        if (contextJson.getMetaData() != null) {
            for (Map.Entry<String, String> metadataEntry : contextJson.getMetaData().entrySet()) {
                try {
                    ContextPropertyDefinition property = ContextPropertyDefinition.findByKey(metadataEntry.getKey());
                    if (property != null) {
                        property.validate(metadataEntry.getValue());
                    }
                } catch (Exception ex) {
                    return new RestResponse(false, ex.getMessage()).asResponseEntity(ErrorCode.INVALID_PROPERTY_VALUE);
                }
            }
        }
        try {
            String replicationGroupName = StringUtils.isEmpty(contextJson.getReplicationGroup()) ? contextJson
                    .getContext() : contextJson.getReplicationGroup();
            AdminReplicationGroup adminReplicationGroup = adminReplicationGroupController.findByName(contextJson
                                                                                                             .getReplicationGroup())
                                                                                         .orElse(null);
            if (adminReplicationGroup == null) {
                if (contextJson.getRoles() == null || contextJson.getRoles().isEmpty()) {
                    return new RestResponse(false, "Missing replication group nodes")
                            .asResponseEntity(ErrorCode.INVALID_CONTEXT_NAME);
                }

                raftServiceFactory.getRaftConfigService()
                                  .createReplicationGroup(replicationGroupName,
                                                          contextJson.getRoles()
                                                                     .stream()
                                                                     .map(n -> ReplicationGroupMember
                                                                             .newBuilder()
                                                                             .setNodeName(n.getNode())
                                                                             .setRole(n.getRole())
                                                                             .build()
                                                                     )
                                                                     .collect(Collectors.toList()));

                String leader = raftLeaderProvider.getLeader(replicationGroupName);
                waitForLeader(replicationGroupName, leader);
            }

            raftServiceFactory.getRaftConfigService()
                              .addContext(replicationGroupName, contextJson.getContext(), contextJson.getMetaData())
                              .get(10, TimeUnit.SECONDS);
            return ResponseEntity.ok(new RestResponse(true, "Context created"));
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return new RestResponse(false, ex.getMessage()).asResponseEntity(ErrorCode.fromException(ex));
        } catch (TimeoutException ex) {
            return ResponseEntity.accepted().body(new RestResponse(true, "Context creation still in progress"));
        } catch (ExecutionException ex) {
            return new RestResponse(false, ex.getCause().getMessage()).asResponseEntity(ErrorCode
                                                                                                .fromException((Exception) ex
                                                                                                        .getCause()));
        } catch (Exception ex) {
            return new RestResponse(false, ex.getMessage()).asResponseEntity(ErrorCode.fromException(ex));
        }
    }

    private void waitForLeader(String replicationGroupName, String leader) {
        long timeout = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10);
        while (leader == null && System.currentTimeMillis() < timeout) {
            try {
                Thread.sleep(100);
                leader = raftLeaderProvider.getLeader(replicationGroupName);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new MessagingPlatformException(ErrorCode.INTERRUPTED, e.getMessage());
            }
        }
        if (leader == null) {
            throw new MessagingPlatformException(ErrorCode.NO_LEADER_AVAILABLE,
                                                 "Unable to find leader for the newly created replication group within the required time");
        }
    }

    @PostMapping(path = "context/init")
    public ResponseEntity<RestResponse> init(@RequestParam(name = "context", required = false) String context,
                                             Principal principal) {
        auditLog.info("[{}] Request to initialize cluster {}.", AuditLog.username(principal), context);
        List<String> contexts = new ArrayList<>();
        if (StringUtils.isEmpty(context)) {
            contexts.add(Topology.DEFAULT_CONTEXT);
        } else {
            contexts.add(context);
        }
        try {
            raftServiceFactory.getLocalRaftConfigService().init(contexts);
            return ResponseEntity.ok(new RestResponse(true, null));
        } catch (Exception ex) {
            return new RestResponse(false, ex.getMessage()).asResponseEntity(ErrorCode.fromException(ex));
        }
    }

    @GetMapping("public/context-properties")
    public ContextPropertyDefinition[] contextProperties(Principal principal) {
        auditLog.info("[{}] Request to get context properties.", AuditLog.username(principal));
        return ContextPropertyDefinition.values();
    }
}
