package io.axoniq.axonserver.grpc;

import io.axoniq.axonserver.component.processor.balancing.TrackingEventProcessor;
import io.axoniq.axonserver.component.processor.balancing.jpa.LoadBalancingStrategy;
import io.axoniq.axonserver.component.processor.balancing.jpa.ProcessorLoadBalancing;
import io.axoniq.axonserver.grpc.internal.Application;
import io.axoniq.axonserver.grpc.internal.ApplicationContextRole;
import io.axoniq.axonserver.grpc.internal.LoadBalanceStrategy;
import io.axoniq.axonserver.grpc.internal.ProcessorLBStrategy;
import io.axoniq.axonserver.grpc.internal.User;
import io.axoniq.platform.application.jpa.ApplicationContext;
import io.axoniq.platform.user.UserRole;

import java.util.List;
import java.util.stream.Collectors;

public class ProtoConverter {

    private ProtoConverter() {
        // not meant to be instantiated
    }

    public static io.axoniq.platform.application.jpa.Application createJpaApplication(Application application) {
        List<ApplicationContext> applicationContexts = application.getRolesPerContextList()
                                                                  .stream()
                                                                  .map(ProtoConverter::createJpaApplicationContext)
                                                                  .collect(Collectors.toList());

        return new io.axoniq.platform.application.jpa.Application(application.getName(),
                                                                  application.getDescription(),
                                                                  application.getTokenPrefix(),
                                                                  application.getToken(),
                                                                  applicationContexts);
    }

    public static ApplicationContext createJpaApplicationContext(ApplicationContextRole applicationContextRole) {
        List<io.axoniq.platform.application.jpa.ApplicationContextRole> roles =
                applicationContextRole.getRolesList()
                                      .stream()
                                      .map(io.axoniq.platform.application.jpa.ApplicationContextRole::new)
                                      .collect(Collectors.toList());

        return new ApplicationContext(applicationContextRole.getContext(), roles);
    }

    public static ApplicationContextRole createApplicationContextRole(ApplicationContext applicationContext) {
        List<String> roles = applicationContext.getRoles()
                                               .stream()
                                               .map(io.axoniq.platform.application.jpa.ApplicationContextRole::getRole)
                                               .collect(Collectors.toList());
        return ApplicationContextRole.newBuilder()
                                     .setContext(applicationContext.getContext())
                                     .addAllRoles(roles)
                                     .build();
    }

    public static Application createApplication(io.axoniq.platform.application.jpa.Application app) {
        Application.Builder builder = Application.newBuilder().setName(app.getName());
        if (app.getDescription() != null) {
            builder.setDescription(app.getDescription());
        }
        if (app.getHashedToken() != null) {
            builder.setToken(app.getHashedToken());
        }
        if (app.getTokenPrefix() != null) {
            builder.setTokenPrefix(app.getTokenPrefix());
        }
        app.getContexts()
           .stream()
           .map(ProtoConverter::createApplicationContextRole)
           .forEach(builder::addRolesPerContext);
        return builder.build();
    }

    public static User createUser(io.axoniq.platform.user.User user) {
        return User.newBuilder()
                   .setName(user.getUserName())
                   .setPassword(user.getPassword() == null ? "" : user.getPassword())
                   .addAllRoles(user.getRoles()
                                    .stream()
                                    .map(UserRole::getRole)
                                    .collect(Collectors.toSet()))
                   .build();
    }

    public static User createUser(String name) {
        return User.newBuilder()
                   .setName(name)
                   .build();
    }

    public static io.axoniq.platform.user.User createJpaUser(User user) {
        return new io.axoniq.platform.user.User(user.getName(),
                                                user.getPassword(),
                                                user.getRolesList().toArray(new String[0]));
    }

    public static LoadBalanceStrategy createLoadBalanceStrategy(LoadBalancingStrategy loadBalancingStrategy) {
        return LoadBalanceStrategy.newBuilder()
                                  .setName(loadBalancingStrategy.name())
                                  .setFactoryBean(loadBalancingStrategy.factoryBean())
                                  .setLabel(loadBalancingStrategy.label())
                                  .build();
    }

    public static LoadBalancingStrategy createJpaLoadBalancingStrategy(LoadBalanceStrategy loadBalanceStrategy) {
        return new LoadBalancingStrategy(loadBalanceStrategy.getName(),
                                         loadBalanceStrategy.getLabel(),
                                         loadBalanceStrategy.getFactoryBean());
    }

    public static ProcessorLBStrategy createProcessorLBStrategy(ProcessorLoadBalancing processorLoadBalancing) {
        return ProcessorLBStrategy.newBuilder()
                                  .setStrategy(processorLoadBalancing.strategy())
                                  .setContext(processorLoadBalancing.processor().context())
                                  .setProcessor(processorLoadBalancing.processor().name())
                                  .setComponent(processorLoadBalancing.processor().component())
                                  .build();
    }

    public static ProcessorLoadBalancing createJpaProcessorLoadBalancing(ProcessorLBStrategy processorLBStrategy) {
        TrackingEventProcessor processor = new TrackingEventProcessor(processorLBStrategy.getProcessor(),
                                                                      processorLBStrategy.getComponent(),
                                                                      processorLBStrategy.getContext());
        return new ProcessorLoadBalancing(processor, processorLBStrategy.getStrategy());
    }
}