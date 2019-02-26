package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.KeepNames;
import io.axoniq.axonserver.config.AccessControlConfiguration;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.config.SslConfiguration;
import io.axoniq.axonserver.features.Feature;
import io.axoniq.axonserver.features.FeatureChecker;
import io.axoniq.axonserver.message.command.CommandDispatcher;
import io.axoniq.axonserver.message.event.EventDispatcher;
import io.axoniq.axonserver.message.query.QueryDispatcher;
import io.axoniq.axonserver.message.query.subscription.SubscriptionMetrics;
import io.axoniq.axonserver.rest.json.NodeConfiguration;
import io.axoniq.axonserver.rest.json.StatusInfo;
import io.axoniq.axonserver.rest.json.UserInfo;
import io.axoniq.axonserver.topology.AxonServerNode;
import io.axoniq.axonserver.topology.Topology;
import io.swagger.annotations.ApiOperation;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;

/**
 * Rest calls to retrieve information about the configuration of Axon Server. Used by UI and CLI.
 * @author Marc Gathier
 */
@RestController("PublicRestController")
@RequestMapping("/v1/public")
public class PublicRestController {

    private final Topology topology;
    private final CommandDispatcher commandDispatcher;
    private final QueryDispatcher queryDispatcher;
    private final EventDispatcher eventDispatcher;
    private final FeatureChecker features;
    private final SslConfiguration sslConfiguration;
    private final AccessControlConfiguration accessControlConfiguration;
    private final Supplier<SubscriptionMetrics> subscriptionMetricsRegistry;


    public PublicRestController(Topology topology,
                                CommandDispatcher commandDispatcher,
                                QueryDispatcher queryDispatcher,
                                EventDispatcher eventDispatcher,
                                FeatureChecker features,
                                MessagingPlatformConfiguration messagingPlatformConfiguration,
                                Supplier<SubscriptionMetrics> subscriptionMetricsRegistry) {
        this.topology = topology;
        this.commandDispatcher = commandDispatcher;
        this.queryDispatcher = queryDispatcher;
        this.eventDispatcher = eventDispatcher;
        this.features = features;
        this.sslConfiguration = messagingPlatformConfiguration.getSsl();
        this.accessControlConfiguration = messagingPlatformConfiguration.getAccesscontrol();
        this.subscriptionMetricsRegistry = subscriptionMetricsRegistry;
    }


    @GetMapping
    @ApiOperation(value="Retrieves all nodes in the cluster that the current node knows about.", notes = "For _admin nodes the result contains all nodes, for non _admin nodes the"
            + "result only contains nodes from contexts available on this node and the _admin nodes.")
    public List<JsonServerNode> getClusterNodes() {
        List<AxonServerNode> nodes = topology.getRemoteConnections();

        nodes.add(topology.getMe());
        return nodes.stream()
                    .map(n -> new JsonServerNode(n, topology.isActive(n)))
                    .sorted(Comparator.comparing(JsonServerNode::getName)).collect(Collectors.toList());
    }

    @GetMapping(path = "me")
    @ApiOperation(value="Retrieves general information on the configuration of the current node, including hostnames and ports for the gRPC and HTTP connections and contexts")
    public NodeConfiguration getNodeConfiguration() {
        NodeConfiguration node = new NodeConfiguration(topology.getMe());
        node.setAuthentication(accessControlConfiguration.isEnabled());
        node.setSsl(sslConfiguration.isEnabled());
        node.setClustered(Feature.CLUSTERING.enabled(features));
        node.setAdminNode(topology.isAdminNode());
        node.setContextNames(topology.getMyContextNames());
        node.setStorageContextNames(topology.getMyStorageContextNames());
        return node;
    }


    @GetMapping(path="mycontexts")
    @ApiOperation(value="Retrieves names for all storage (non admin) contexts for the current node")
    public Iterable<String> getMyContextList() {
        return topology.getMyStorageContextNames();
    }



    @GetMapping(path = "license")
    @ApiOperation(value="Retrieves license information")
    public LicenseInfo licenseInfo() {
        LicenseInfo licenseInfo = new LicenseInfo();
        licenseInfo.setExpiryDate(features.getExpiryDate());
        licenseInfo.setEdition(features.getEdition());
        licenseInfo.setLicensee(features.getLicensee());
        licenseInfo.setFeatureList(features.getFeatureList());


        return licenseInfo;
    }

    @GetMapping(path = "status")
    @ApiOperation(value="Retrieves status information, used by UI")
    public StatusInfo status() {
        SubscriptionMetrics subscriptionMetrics = this.subscriptionMetricsRegistry.get();
        StatusInfo statusInfo = new StatusInfo();
        statusInfo.setNrOfCommands(commandDispatcher.getNrOfCommands());
        statusInfo.setNrOfQueries(queryDispatcher.getNrOfQueries());
        statusInfo.setNrOfEvents(eventDispatcher.getNrOfEvents());
        statusInfo.setNrOfSnapshots(eventDispatcher.getNrOfSnapshots());
        statusInfo.setEventTrackers(eventDispatcher.eventTrackerStatus());
        statusInfo.setNrOfSubscriptionQueries(subscriptionMetrics.totalCount());
        statusInfo.setNrOfActiveSubscriptionQueries(subscriptionMetrics.activesCount());
        statusInfo.setNrOfSubscriptionQueriesUpdates(subscriptionMetrics.updatesCount());
        return statusInfo;
    }


    @GetMapping(path = "user")
    @ApiOperation(value="Retrieves information on the user logged in in the current Http Session")
    public UserInfo userInfo(HttpServletRequest request) {
        if (request.getUserPrincipal() instanceof Authentication) {
            Authentication token = (Authentication) request.getUserPrincipal();
            return new UserInfo(token.getName(),
                                token.getAuthorities().stream().map(GrantedAuthority::getAuthority).collect(Collectors.toSet()));
        }

        return null;
    }


    @KeepNames
    public static class JsonServerNode {

        private final AxonServerNode wrapped;
        private final boolean connected;

        public JsonServerNode(AxonServerNode axonServerNode, boolean connected) {
            this.wrapped = axonServerNode;
            this.connected = connected;
        }


        public String getHostName() {
            return wrapped.getHostName();
        }

        public Integer getGrpcPort() {
            return wrapped.getGrpcPort();
        }

        public String getInternalHostName() {
            return wrapped.getInternalHostName();
        }

        public Integer getGrpcInternalPort() {
            return wrapped.getGrpcInternalPort();
        }

        public Integer getHttpPort() {
            return wrapped.getHttpPort();
        }

        public String getName() {
            return wrapped.getName();
        }

        public boolean isConnected() {
            return connected;
        }
    }
}
