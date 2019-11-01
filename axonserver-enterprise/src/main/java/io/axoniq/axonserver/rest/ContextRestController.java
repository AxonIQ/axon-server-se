package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.config.FeatureChecker;
import io.axoniq.axonserver.enterprise.cluster.RaftConfigServiceFactory;
import io.axoniq.axonserver.enterprise.cluster.RaftLeaderProvider;
import io.axoniq.axonserver.enterprise.context.ContextController;
import io.axoniq.axonserver.enterprise.context.ContextNameValidation;
import io.axoniq.axonserver.enterprise.jpa.Context;
import io.axoniq.axonserver.enterprise.task.DeleteNodeFromContextJob;
import io.axoniq.axonserver.enterprise.topology.ClusterTopology;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.grpc.cluster.Role;
import io.axoniq.axonserver.licensing.Feature;
import io.axoniq.axonserver.rest.json.RestResponse;
import io.axoniq.axonserver.topology.Topology;
import io.axoniq.axonserver.util.StringUtils;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.validation.Valid;

import static io.axoniq.axonserver.RaftAdminGroup.isAdmin;

/**
 * @author Marc Gathier
 */
@RestController
@CrossOrigin
@RequestMapping("/v1")
public class ContextRestController {

    private final RaftConfigServiceFactory raftServiceFactory;
    private final RaftLeaderProvider raftLeaderProvider;
    private final ContextController contextController;
    private final ClusterTopology clusterTopology;
    private final DeleteNodeFromContextJob deleteNodeFromContextJob;
    private final FeatureChecker limits;
    private final Predicate<String> contextNameValidation = new ContextNameValidation();
    private final Logger logger = LoggerFactory.getLogger(ContextRestController.class);

    public ContextRestController(RaftConfigServiceFactory raftServiceFactory,
                                 RaftLeaderProvider raftLeaderProvider,
                                 ContextController contextController,
                                 ClusterTopology clusterTopology,
                                 DeleteNodeFromContextJob deleteNodeFromContextJob,
                                 FeatureChecker limits) {
        this.raftServiceFactory = raftServiceFactory;
        this.raftLeaderProvider = raftLeaderProvider;
        this.contextController = contextController;
        this.clusterTopology = clusterTopology;
        this.deleteNodeFromContextJob = deleteNodeFromContextJob;
        this.limits = limits;
    }

    @GetMapping(path = "public/context")
    public List<ContextJSON> getContexts() {
        return contextController.getContexts().map(context -> {
            ContextJSON json = new ContextJSON(context.getName());
            json.setChangePending(context.isChangePending());
            if (context.getPendingSince() != null) {
                json.setPendingSince(context.getPendingSince().getTime());
            }
            json.setLeader(raftLeaderProvider.getLeader(context.getName()));
            json.setNodes(context.getNodes().stream().map(n -> n.getClusterNode().getName()).sorted()
                                 .collect(Collectors.toList()));
            json.setRoles(context.getNodes().stream().map(ContextJSON.NodeAndRole::new).sorted()
                                 .collect(Collectors.toList()));
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
            @RequestParam(name = "includeAdmin", required = false, defaultValue = "false") boolean includeAdmin) {
        if (clusterTopology.isAdminNode()) {
            return contextController.getContexts()
                                    .map(Context::getName)
                                    .filter(name -> includeAdmin || !isAdmin(name))
                                    .sorted()
                                    .collect(Collectors.toList());
        } else {
            return clusterTopology.getMyStorageContextNames();
        }
    }

    @DeleteMapping(path = "context/{name}")
    public ResponseEntity<RestResponse> deleteContext(@PathVariable("name") String name) {
        logger.info("Delete request received for context: {}", name);
        try {
            if (name.startsWith("_")) {
                return new RestResponse(false, String.format(
                        "Cannot delete internal context %s",
                        name))
                        .asResponseEntity(ErrorCode.CANNOT_DELETE_INTERNAL_CONTEXT);
            }
            raftServiceFactory.getRaftConfigService().deleteContext(name);
            return ResponseEntity.accepted()
                                 .body(new RestResponse(true,
                                                        "Accepted delete request"));
        } catch (Exception ex) {
            return new RestResponse(false, ex.getMessage()).asResponseEntity(ErrorCode.fromException(ex));
        }
    }

    @PostMapping(path = "context/{context}/{node}")
    public ResponseEntity<RestResponse> updateNodeRoles(@PathVariable("context") String name,
                                                        @PathVariable("node") String node,
                                                        @RequestParam(value = "role", required = false, defaultValue = "PRIMARY") String role) {
        logger.info("Add node request received for node: {} - and context: {}", node, name);
        try {
            raftServiceFactory.getRaftConfigService().addNodeToContext(name,
                                                                       node,
                                                                       role == null ? Role.PRIMARY : Role
                                                                               .valueOf(role));
            return ResponseEntity.accepted()
                                 .body(new RestResponse(true,
                                                        "Started to add node to context. This may take some time depending on the number of events already in the context"));
        } catch (Exception ex) {
            return new RestResponse(false, ex.getMessage()).asResponseEntity(ErrorCode.fromException(ex));
        }
    }

    @DeleteMapping(path = "context/{context}/{node}")
    public ResponseEntity<RestResponse> deleteNodeFromContext(@PathVariable("context") String name,
                                                              @PathVariable("node") String node,
                                                              @RequestParam(name = "preserveEventStore", required = false, defaultValue = "false") boolean preserveEventStore) {
        logger.warn("Delete node request received for node: {} - and context: {} {}",
                    node,
                    name,
                    preserveEventStore ? "- Keeping event store" : "");
        try {
            deleteNodeFromContextJob.deleteNodeFromContext(name, node, preserveEventStore);
            return ResponseEntity.accepted()
                                 .body(new RestResponse(true,
                                                        "Accepted delete request"));
        } catch (Exception ex) {
            return new RestResponse(false, ex.getMessage()).asResponseEntity(ErrorCode.fromException(ex));
        }
    }

    @PostMapping(path = "context")
    public ResponseEntity<RestResponse> addContext(@RequestBody @Valid ContextJSON contextJson) {
        logger.info("Add context request received for context: {}", contextJson.getContext());
        if (!Feature.MULTI_CONTEXT.enabled(limits)) {
            return new RestResponse(false, "License does not allow creating contexts")
                    .asResponseEntity(ErrorCode.CONTEXT_CREATION_NOT_ALLOWED);
        }
        if (contextJson.getNodes() == null || contextJson.getNodes().isEmpty()) {
            return new RestResponse(false, "Add at least one node for context")
                    .asResponseEntity(ErrorCode.CONTEXT_CREATION_NOT_ALLOWED);
        }

        if (!contextNameValidation.test(contextJson.getContext())) {
            return new RestResponse(false, "Invalid context name").asResponseEntity(ErrorCode.INVALID_CONTEXT_NAME);
        }

        try {
            raftServiceFactory.getRaftConfigService().addContext(contextJson.getContext(), contextJson.getNodes());
            return ResponseEntity.ok(new RestResponse(true, null));
        } catch (Exception ex) {
            return new RestResponse(false, ex.getMessage()).asResponseEntity(ErrorCode.fromException(ex));
        }
    }

    @PostMapping(path = "context/init")
    public ResponseEntity<RestResponse> init(@RequestParam(name = "context", required = false) String context) {
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

    /**
     * Returns a set of roles that can be assigned to nodes in a context.
     *
     * @return set of role names
     */
    @GetMapping(path = "context/roles")
    public Set<String> roles() {
        return Arrays.stream(Role.values())
                     .filter(r -> !Role.UNRECOGNIZED.equals(r))
                     .map(Enum::name)
                     .collect(Collectors.toSet());
    }
}
