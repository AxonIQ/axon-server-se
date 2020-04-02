package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.ClusterTagsCache;
import io.axoniq.axonserver.KeepNames;
import io.axoniq.axonserver.config.FeatureChecker;
import io.axoniq.axonserver.enterprise.cluster.ClusterController;
import io.axoniq.axonserver.enterprise.cluster.RaftConfigServiceFactory;
import io.axoniq.axonserver.enterprise.context.ContextNameValidation;
import io.axoniq.axonserver.enterprise.jpa.ClusterNode;
import io.axoniq.axonserver.enterprise.taskscheduler.task.PrepareUpdateLicenseTask;
import io.axoniq.axonserver.exception.ErrorCode;
import io.axoniq.axonserver.exception.MessagingPlatformException;
import io.axoniq.axonserver.grpc.internal.ContextRole;
import io.axoniq.axonserver.grpc.internal.NodeInfo;
import io.axoniq.axonserver.licensing.Feature;
import io.axoniq.axonserver.rest.json.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

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
    private final PrepareUpdateLicenseTask prepareUpdateLicenseTask;

    public ClusterRestController(ClusterController clusterController,
                                 RaftConfigServiceFactory raftServiceFactory,
                                 FeatureChecker limits,
                                 ClusterTagsCache clusterTagsCache,
                                 PrepareUpdateLicenseTask prepareUpdateLicenseTask) {
        this.clusterController = clusterController;
        this.raftServiceFactory = raftServiceFactory;
        this.limits = limits;
        this.prepareUpdateLicenseTask = prepareUpdateLicenseTask;
    }


    @PostMapping
    public ResponseEntity<RestResponse> add(@Valid @RequestBody ClusterJoinRequest jsonClusterNode) {
        if (!Feature.CLUSTERING.enabled(limits)) {
            return new RestResponse(false, "License does not allow clustering of Axon servers")
                    .asResponseEntity(ErrorCode.CLUSTER_NOT_ALLOWED);
        }

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
            raftServiceFactory.getRaftConfigServiceStub(jsonClusterNode.internalHostName,
                                                        jsonClusterNode.internalGrpcPort)
                              .joinCluster(nodeInfoBuilder.build());

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

    @GetMapping("/test")
    public void distributeLicense() {

        String license = "#AxonIQ License File generated on 2019-10-18\n" +
                "#Fri Oct 18 08:00:32 UTC 2019\n" +
                "license_key_id=779a356b-e226-4dc8-8d43-32937549d081\n" +
                "signature=sOMWdskQcXgqf1HghdGWeJo1s55T0jyYK2Uv7bFOJo6R4t77PsuMDERc1Il+RSfaLC/X5uMNoCca8uEdIbgBAk0K/JliMP8Ac69e+kziEaCvuM44vfJw3cPUngZcgrH5b7J790KQ4kxmEv8gnm6FbnvFnSwNAqwHqTqWDfUoBI02JC4DmRl2K1DTa0cE6qXvGNnHdt9vRfSoFVq0wzFbBd0gDiZWD0qsaTntskqtHtityrN8LcY90MGbwrJksg7Hbywtz7tinGBEmHohtP9ELOkHJ0TkHWt7/yUliBVypw08PKl93BSz4qg5jHovsWxFfUjbc3gxlyQ8Rq8sISC+zu/mZMr+Owmx1wBT/s1BBiPqYjNMyYPKV6Vdo3OLdWpfy27feWlxTJGEmKif99muvJO+xB7P0KORdPjgO85Mo6zeq4Hn5ApWRdHTexmc3ZekGejPNZJ38114wn0ND3DYmachUJE7oF4DZwNluPqfnp/GXYWs13LVraDg2/S+zaQHFYLaRZKuzdgf/7CWDdqJZd2yBxe+2pt5TWqc1N1ePknBWszdLaqtI/meCOwRVlJt1jXlk7RRPMPZcR4LkNgH8O0ETae/aNtM2elZczk4xeDUcBpmXX050cunosMTlcxxdrVyBhJijA2A9Krl9MH3PPhcacfqspUX/tYgm3IGDwo\\=\n" +
                "reference=License for SAAS platform\n" +
                "contexts=1000\n" +
                "expiry_date=2025-10-18\n" +
                "contact=\n" +
                "edition=Enterprise\n" +
                "issue_date=2019-10-18\n" +
                "grace_date=2025-11-01\n" +
                "product=AxonServer\n" +
                "licensee=AxonIQ B.V.\n" +
                "clusterNodes=1000\n";

        prepareUpdateLicenseTask.executeAsync(license.getBytes());
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
