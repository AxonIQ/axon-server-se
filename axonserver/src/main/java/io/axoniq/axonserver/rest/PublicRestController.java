/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.config.AccessControlConfiguration;
import io.axoniq.axonserver.config.FeatureChecker;
import io.axoniq.axonserver.config.MessagingPlatformConfiguration;
import io.axoniq.axonserver.config.SslConfiguration;
import io.axoniq.axonserver.message.command.CommandDispatcher;
import io.axoniq.axonserver.message.event.EventDispatcher;
import io.axoniq.axonserver.message.query.QueryDispatcher;
import io.axoniq.axonserver.message.query.subscription.SubscriptionMetrics;
import io.axoniq.axonserver.rest.json.NodeConfiguration;
import io.axoniq.axonserver.rest.json.StatusInfo;
import io.axoniq.axonserver.rest.json.UserInfo;
import io.axoniq.axonserver.rest.svg.mapping.AxonServer;
import io.axoniq.axonserver.topology.Topology;
import io.axoniq.axonserver.version.VersionInfo;
import io.axoniq.axonserver.version.VersionInfoProvider;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.servlet.http.HttpServletRequest;

/**
 * Rest calls to retrieve information about the configuration of Axon Server. Used by UI and CLI.
 * @author Marc Gathier
 */
@RestController("PublicRestController")
@RequestMapping("/v1/public")
public class PublicRestController {

    private final Function<String, Stream<AxonServer>> axonServerProvider;
    private final Topology topology;
    private final CommandDispatcher commandDispatcher;
    private final QueryDispatcher queryDispatcher;
    private final EventDispatcher eventDispatcher;
    private final FeatureChecker features;
    private final SslConfiguration sslConfiguration;
    private final AccessControlConfiguration accessControlConfiguration;
    private final VersionInfoProvider versionInfoSupplier;
    private final Supplier<SubscriptionMetrics> subscriptionMetricsRegistry;
    private final boolean extensionsEnabled;

    @Value("${axoniq.axonserver.devmode.enabled:false}")
    private boolean isDevelopmentMode;

    public PublicRestController(Function<String, Stream<AxonServer>> axonServerProvider,
                                Topology topology,
                                CommandDispatcher commandDispatcher,
                                QueryDispatcher queryDispatcher,
                                EventDispatcher eventDispatcher,
                                FeatureChecker features,
                                MessagingPlatformConfiguration messagingPlatformConfiguration,
                                VersionInfoProvider versionInfoSupplier,
                                Supplier<SubscriptionMetrics> subscriptionMetricsRegistry) {
        this.axonServerProvider = axonServerProvider;
        this.topology = topology;
        this.commandDispatcher = commandDispatcher;
        this.queryDispatcher = queryDispatcher;
        this.eventDispatcher = eventDispatcher;
        this.features = features;
        this.sslConfiguration = messagingPlatformConfiguration.getSsl();
        this.accessControlConfiguration = messagingPlatformConfiguration.getAccesscontrol();
        this.extensionsEnabled = messagingPlatformConfiguration.isExtensionsEnabled();
        this.versionInfoSupplier = versionInfoSupplier;
        this.subscriptionMetricsRegistry = subscriptionMetricsRegistry;
    }


    @GetMapping
    @ApiOperation(value="Retrieves all nodes in the cluster that the current node knows about.", notes = "For _admin nodes the result contains all nodes, for non _admin nodes the"
            + "result only contains nodes from contexts available on this node and the _admin nodes.")
    public List<JsonServerNode> getClusterNodes() {
        return axonServerProvider.apply(null).map(n -> new JsonServerNode(n))
                                 .sorted(Comparator.comparing(JsonServerNode::getName)).collect(Collectors.toList());
    }

    @GetMapping(path = "me")
    @ApiOperation(value="Retrieves general information on the configuration of the current node, including hostnames and ports for the gRPC and HTTP connections and contexts")
    public NodeConfiguration getNodeConfiguration() {
        NodeConfiguration node = new NodeConfiguration(topology.getMe());
        node.setAuthentication(accessControlConfiguration.isEnabled());
        node.setSsl(sslConfiguration.isEnabled());
        node.setAdminNode(topology.isAdminNode());
        node.setDevelopmentMode(isDevelopmentMode);
        node.setContextNames(topology.getMyContextNames());
        node.setStorageContextNames(topology.getMyStorageContextNames());
        node.setClustered(features.isEnterprise());
        node.setExtensionsEnabled(extensionsEnabled);
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
    public StatusInfo status(@RequestParam(value = "context", defaultValue = Topology.DEFAULT_CONTEXT, required = false) String context) {
        SubscriptionMetrics subscriptionMetrics = this.subscriptionMetricsRegistry.get();
        StatusInfo statusInfo = new StatusInfo();
        statusInfo.setCommandRate(commandDispatcher.commandRate(context));
        statusInfo.setQueryRate(queryDispatcher.queryRate(context));
        if( ! context.startsWith("_")) {
            statusInfo.setEventRate(eventDispatcher.eventRate(context));
            statusInfo.setSnapshotRate(eventDispatcher.snapshotRate(context));
            statusInfo.setNrOfEvents(eventDispatcher.getNrOfEvents(context));
            statusInfo.setEventTrackers(eventDispatcher.eventTrackerStatus(context));
        }
        statusInfo.setNrOfActiveSubscriptionQueries(subscriptionMetrics.activesCount());
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

    @GetMapping(path = "version")
    @ApiOperation(value = "Retrieves version information of the product")
    public VersionInfo versionInfo() {
        return versionInfoSupplier.get();
    }


    public static class JsonServerNode {

        private final AxonServer wrapped;

        JsonServerNode(AxonServer n) {
            wrapped = n;

        }


        public String getHostName() {
            return wrapped.node().getHostName();
        }

        public Integer getGrpcPort() {
            return wrapped.node().getGrpcPort();
        }

        public String getInternalHostName() {
            return wrapped.node().getInternalHostName();
        }

        public Integer getGrpcInternalPort() {
            return wrapped.node().getGrpcInternalPort();
        }

        public Integer getHttpPort() {
            return wrapped.node().getHttpPort();
        }

        public String getName() {
            return wrapped.node().getName();
        }

        public boolean isConnected() {
            return wrapped.isActive();
        }
    }
}
