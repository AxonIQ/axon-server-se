package io.axoniq.axonserver.rest;

import io.axoniq.axonserver.rest.svg.mapping.Application;
import io.axoniq.axonserver.rest.svg.mapping.AxonServer;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Provides current cluster state, applications and connected nodes
 * as serializable POJO's
 *
 * @author Stefan Dragisic
 * @since 4.4.4
 */

@Component
public class AxonServersOverviewProvider {

    private final Function<String, Stream<Application>> applicationProvider;
    private final Function<String, Stream<AxonServer>> axonServerProvider;

    public AxonServersOverviewProvider(Function<String, Stream<Application>> applicationProvider,
                                       Function<String, Stream<AxonServer>> axonServerProvider) {
        this.applicationProvider = applicationProvider;
        this.axonServerProvider = axonServerProvider;
    }

    public ApplicationsAndNodes applicationsAndNodes() {
        return applicationsAndNodes(null);
    }

    public ApplicationsAndNodes applicationsAndNodes(String context) {
        return new ApplicationsAndNodes(listApplications(context), listNodes(context));
    }

    public List<ConnectedApplication> listApplications(String context) {
        return applicationProvider.apply(context)
                                  .map(ConnectedApplication::new)
                                  .sorted(Comparator.comparing(ConnectedApplication::getName)).collect(Collectors
                                                                                                               .toList());
    }

    public List<ServerNode> listNodes(String context) {
        return axonServerProvider.apply(context)
                                 .map(ServerNode::new)
                                 .filter(s -> s.hasContext(context))
                                 .sorted(Comparator.comparing(ServerNode::getName)).collect(Collectors.toList());
    }


    public static class ApplicationsAndNodes {

        List<ConnectedApplication> applications;
        List<ServerNode> nodes;

        public ApplicationsAndNodes(List<ConnectedApplication> applications, List<ServerNode> nodes) {
            this.applications = applications;
            this.nodes = nodes;
        }

        public List<ConnectedApplication> getApplications() {
            return applications;
        }

        public List<ServerNode> getNodes() {
            return nodes;
        }

    }

    public static class ConnectedApplication {

        private final Application wrapped;

        public ConnectedApplication(Application wrapped) {
            this.wrapped = wrapped;
        }

        public String getName() {
            return wrapped.name();
        }

        public String getComponent() {
            return wrapped.component();
        }

        public String getContext() {
            return wrapped.context();
        }

        public int getInstances() {
            return wrapped.instances();
        }

        public Iterable<String> getConnectedHubNodes() {
            return wrapped.connectedHubNodes();
        }
    }

    public static class ServerNode {

        private final AxonServer wrapped;

        ServerNode(AxonServer n) {
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

        public Map<String,String> getTags() {
            return wrapped.tags();
        }

        public Iterable<String> getContexts() {
            return wrapped.contexts();
        }

        public boolean isConnected() {
            return wrapped.isActive();
        }

        public boolean isAdmin() {
            return wrapped.isAdminLeader();
        }

        public boolean hasContext(String context) {
            if (context == null) {
                return true;
            }
            return StreamSupport.stream(getContexts().spliterator(), false).anyMatch(s -> s.equals(context));
        }
    }
}
