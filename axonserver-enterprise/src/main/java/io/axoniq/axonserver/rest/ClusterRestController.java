package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.KeepNames;
import io.axoniq.axonserver.config.FeatureChecker;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.DistributeLicenseService;
import io.axoniq.axonserver.enterprise.cluster.RaftConfigServiceFactory;
import io.axoniq.axonserver.enterprise.cluster.events.ClusterEvents;
import io.axoniq.axonserver.enterprise.context.ContextNameValidation;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.internal.ContextRole;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.axoniq.axonserver.grpc.internal.UpdateLicense;
import io.axoniq.axonserver.licensing.Feature;
import io.axoniq.axonserver.rest.json.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import static io.axoniq.axonserver.RaftAdminGroup.isAdmin;

/**
 * @author Marc Gathier
 */
@RestController("ClusterRestController")
@RequestMapping("/v1/cluster")
public class ClusterRestController {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static final String CONTEXT_NONE = "_none";

    private final ClusterController clusterController;
    private final RaftConfigServiceFactory raftServiceFactory;
    private final FeatureChecker limits;
    private final Predicate<String> contextNameValidation = new ContextNameValidation();
    private final DistributeLicenseService distributeLicenseService;
    private final ApplicationEventPublisher eventPublisher;

    public ClusterRestController(ClusterController clusterController,
                                 RaftConfigServiceFactory raftServiceFactory,
                                 FeatureChecker limits,
                                 DistributeLicenseService distributeLicenseService, ApplicationEventPublisher eventPublisher) {
        this.clusterController = clusterController;
        this.raftServiceFactory = raftServiceFactory;
        this.limits = limits;
        this.distributeLicenseService = distributeLicenseService;
        this.eventPublisher = eventPublisher;
    }


    @PostMapping
    public ResponseEntity<RestResponse> add(@Valid @RequestBody ClusterJoinRequest jsonClusterNode) {

        NodeInfo.Builder nodeInfoBuilder = NodeInfo.newBuilder(clusterController.getMe().toNodeInfo());
        String context = jsonClusterNode.getContext();
        // Check for both context and noContext
        if (context != null && !context.isEmpty()) {
            if ((jsonClusterNode.getNoContexts() != null) && jsonClusterNode.getNoContexts()) {
                throw new MessagingPlatformException(ErrorCode.INVALID_CONTEXT_NAME,
                        "Cannot combine joining context with noContexts.");
            } else if (!isAdmin(context) && !contextNameValidation.test(context)) {
                throw new MessagingPlatformException(ErrorCode.INVALID_CONTEXT_NAME,
                                                     "Invalid context name: " + context);
            }
            logger.debug("add(): Registering myself and adding me to context \"{}\".", context);
            nodeInfoBuilder.addContexts(ContextRole.newBuilder().setName(context).build());
        } else if ((jsonClusterNode.getNoContexts() != null) && jsonClusterNode.getNoContexts()) {
            logger.debug("add(): Registering myself and adding me to no contexts.");
            nodeInfoBuilder.addContexts(ContextRole.newBuilder().setName(CONTEXT_NONE).build());
        } else {
            logger.debug("add(): Registering myself and adding me to all contexts.");
        }

        // Check if node is already registered in a cluster
        if (clusterController.nodes().count() > 1) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                                 .body(new RestResponse(false,
                                                        "Node already registered in a cluster"));
        }

        try {
            UpdateLicense updateLicense = raftServiceFactory.getRaftConfigServiceStub(jsonClusterNode.internalHostName,
                    jsonClusterNode.internalGrpcPort)
                    .joinCluster(nodeInfoBuilder.build());

            eventPublisher.publishEvent(new ClusterEvents.LicenseUpdated(updateLicense.getLicense().toByteArray()));

            return ResponseEntity.accepted()
                                 .body(new RestResponse(true,
                                                        "Accepted join request, may take a while to process"));
        } catch (Exception ex) {
            return new RestResponse(false, ex.getMessage()).asResponseEntity(ErrorCode.fromException(ex));
        }
    }


    @DeleteMapping( path = "{name}")
    public void deleteNode(@PathVariable("name") String name) {
        if( !Feature.CLUSTERING.enabled(limits) ) {
            throw new MessagingPlatformException(ErrorCode.CLUSTER_NOT_ALLOWED, "License does not allow clustering of Axon servers");
        }
        raftServiceFactory.getRaftConfigService().deleteNode(name);
    }


    @GetMapping
    public List<JsonClusterNode> list() {
        return clusterController
                .nodes()
                .map(e -> JsonClusterNode.from(e, clusterController.isActive(e.getName())))
                .collect(Collectors.toList());
    }

    @PostMapping("/upload-license")
    public void distributeLicense(@RequestParam("licenseFile") MultipartFile licenseFile) throws IOException {
        logger.info("New license uploaded, performing license update...");
        distributeLicenseService.distributeLicense(licenseFile.getBytes());
    }

    @GetMapping(path="{name}")
    public JsonClusterNode getOne(@PathVariable("name") String name) {
        ClusterNode node = clusterController.getNode(name);
        if( node == null ) throw new MessagingPlatformException(ErrorCode.NO_SUCH_NODE, "Node " + name + " not found");

        return JsonClusterNode.from(node, clusterController.isActive(name));
    }

    @KeepNames
    public static class JsonClusterNode {
        private String name;
        private String internalHostName;
        private Integer internalGrpcPort;
        private String hostName;
        private Integer grpcPort;
        private Integer httpPort;
        private boolean connected;
        private Map<String,String> tags;

        public boolean isConnected() {
            return connected;
        }

        public void setConnected(boolean connected) {
            this.connected = connected;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getInternalHostName() {
            return internalHostName;
        }

        public void setInternalHostName(String internalHostName) {
            this.internalHostName = internalHostName;
        }

        public String getHostName() {
            return hostName;
        }

        public void setHostName(String hostName) {
            this.hostName = hostName;
        }

        public Integer getHttpPort() {
            return httpPort;
        }

        public void setHttpPort(Integer httpPort) {
            this.httpPort = httpPort;
        }

        public Integer getInternalGrpcPort() {
            return internalGrpcPort;
        }

        public void setInternalGrpcPort(Integer internalGrpcPort) {
            this.internalGrpcPort = internalGrpcPort;
        }

        public Integer getGrpcPort() {
            return grpcPort;
        }

        public void setGrpcPort(Integer grpcPort) {
            this.grpcPort = grpcPort;
        }

        public Map<String,String> getTags(){
            return tags;
        }

        public void setTags(Map<String,String> tags){
            this.tags = tags;
        }

        public static JsonClusterNode from(ClusterNode jpaClusterNode, boolean connected) {
            JsonClusterNode clusterNode = new JsonClusterNode();
            clusterNode.name = jpaClusterNode.getName();
            clusterNode.internalHostName = jpaClusterNode.getInternalHostName();
            clusterNode.internalGrpcPort = jpaClusterNode.getGrpcInternalPort();
            clusterNode.hostName = jpaClusterNode.getHostName();
            clusterNode.grpcPort = jpaClusterNode.getGrpcPort();
            clusterNode.httpPort = jpaClusterNode.getHttpPort();
            clusterNode.connected = connected;
            clusterNode.tags = jpaClusterNode.getTags();
            return clusterNode;
        }
    }

    @KeepNames
    public static class ClusterJoinRequest {
        @NotNull(message = "missing required field: internalHostName")
        private String internalHostName;
        @NotNull(message = "missing required field: internalGrpcPort")
        private Integer internalGrpcPort;

        private String context;
        private Boolean noContexts;

        public String getInternalHostName() {
            return internalHostName;
        }

        public void setInternalHostName(String internalHostName) {
            this.internalHostName = internalHostName;
        }

        public Integer getInternalGrpcPort() {
            return internalGrpcPort;
        }

        public void setInternalGrpcPort(@NotNull Integer internalGrpcPort) {
            this.internalGrpcPort = internalGrpcPort;
        }

        /**
         * Return the context this node should join. If {@code null} and {@link #noContexts} equals {@code true},
         * then the node should join no context. If both are {@code null}, the node should join all contexts.
         *
         * @return the context to join.
         */
        public String getContext() {
            return context;
        }

        /**
         * Set the context to join.
         *
         * @param context the name of the context, or {@code null} for all/none. See {@link #getContext()} and {@link #getNoContexts()}.
         */
        public void setContext(String context) {
            this.context = context;
        }

        /**
         * Return the (optional) field to indicate if this node should be joined to no contexts. A value of {@code null}
         * indicates that the old behaviour is expected, which means that the node will be added to a single or all
         * contexts known to the "_admin" leader. If {@code true}, {@link #context} should not be used.
         *
         * @return {@code true} if the node should be joined to no contexts.
         */
        public Boolean getNoContexts() {
            return noContexts;
        }

        /**
         * Set the (optional) {@link #noContexts} field. Typically, only {@code true} or {@code null} make sense.
         *
         * @param noContexts the value for this field.
         */
        public void setNoContexts(Boolean noContexts) {
            this.noContexts = noContexts;
        }
    }
}
