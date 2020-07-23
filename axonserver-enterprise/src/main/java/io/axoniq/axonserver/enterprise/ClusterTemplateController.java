package io.axoniq.axonserver.enterprise;

import io.axoniq.axonserver.access.application.AdminApplicationContext;
import io.axoniq.axonserver.access.application.AdminApplicationContextRole;
import io.axoniq.axonserver.access.application.AdminApplicationRepository;
import io.axoniq.axonserver.access.jpa.UserRole;
import io.axoniq.axonserver.access.user.UserRepository;
import io.axoniq.axonserver.enterprise.config.ClusterTemplate;
import io.axoniq.axonserver.enterprise.jpa.AdminReplicationGroupRepository;
import io.axoniq.axonserver.util.StringUtils;
import org.springframework.stereotype.Controller;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.logging.log4j.util.Strings.EMPTY;


/**
 * Constructs ClusterTemplate POJO from ControlDB data
 *
 * @author Stefan Dragisic
 * @since 4.4
 */
@Controller("ClusterTemplateController")
public class ClusterTemplateController {

    private static final String TOKEN_HOLDER = "*your secret token here*";
    private static final String UNDEFINED = "UNDEFINED";
    private static final String PASSWORD_HOLDER = "*your secret user password here*";

    private final AdminReplicationGroupRepository adminReplicationGroupRepository;

    private final AdminApplicationRepository adminApplicationRepository;

    private final UserRepository userRepository;

    public ClusterTemplateController(AdminReplicationGroupRepository adminReplicationGroupRepository,
                                     AdminApplicationRepository adminApplicationRepository,
                                     UserRepository userRepository) {

        this.adminReplicationGroupRepository = adminReplicationGroupRepository;
        this.adminApplicationRepository = adminApplicationRepository;

        this.userRepository = userRepository;
    }

    /**
     * Constructs ClusterTemplate POJO from ControlDB data
     */
    public ClusterTemplate buildTemplate() {
        ClusterTemplate clusterTemplate = new ClusterTemplate();

        String first = buildFirst();
        List<ClusterTemplate.Application> applications = buildApplications();
        List<ClusterTemplate.User> users = buildUser();
        List<ClusterTemplate.ReplicationsGroup> replicationsGroups = buildReplicationGroups();

        clusterTemplate.setFirst(first);
        clusterTemplate.setApplications(applications);
        clusterTemplate.setUsers(users);
        clusterTemplate.setReplicationsGroups(replicationsGroups);
        return clusterTemplate;
    }

    private List<ClusterTemplate.ReplicationsGroup> buildReplicationGroups() {
        return adminReplicationGroupRepository
                .findAll()
                .stream()
                .map(rp -> new ClusterTemplate.ReplicationsGroup(
                        rp.getName(),
                        rp.getMembers()
                                .stream()
                                .map(it ->
                                        new ClusterTemplate.ReplicationGroupRole(
                                                it.getClusterNode().getName(), it.getRole().name())
                                )
                                .collect(Collectors.toList()),
                        rp.getContexts()
                                .stream()
                                .map(c-> new ClusterTemplate.Context(c.getName(),c.getMetaDataMap()))
                                .collect(Collectors.toList())
                ))
                .collect(Collectors.toList());
    }

    private String buildFirst() {
        return adminReplicationGroupRepository
                .findAll()
                .stream()
                .flatMap(rp->rp.getMembers().stream())
                .findFirst()
                .map(member->member.getClusterNode().getInternalHostName())
                .orElse(UNDEFINED);
    }

    private List<ClusterTemplate.Application> buildApplications() {
        return adminApplicationRepository
                .findAll()
                .stream()
                .map(app -> new ClusterTemplate.Application(
                        app.getName(),
                        StringUtils.getOrDefault(app.getDescription(), EMPTY),
                        app.getContexts()
                                .stream()
                                .collect(Collectors.groupingBy(AdminApplicationContext::getContext))
                                .entrySet()
                                .stream()
                                .map(ks ->
                                        new ClusterTemplate.ApplicationRole(
                                                ks.getKey(),
                                                ks.getValue()
                                                        .stream()
                                                        .flatMap(it -> it.getRoles()
                                                                .stream()
                                                                .map(AdminApplicationContextRole::getRole))
                                                        .collect(Collectors.toList())
                                        )
                                ).collect(Collectors.toList())
                        ,TOKEN_HOLDER))
                .collect(Collectors.toList());

    }

    private List<ClusterTemplate.User> buildUser() {
        return userRepository
                .findAll()
                .stream()
                .map(u ->
                        new ClusterTemplate.User(
                                u.getUserName(),
                                u.getRoles()
                                        .stream()
                                        .collect(Collectors.groupingBy(UserRole::getContext))
                                        .entrySet()
                                        .stream()
                                        .map(ks ->
                                                new ClusterTemplate.UserRole(
                                                        ks.getKey(),
                                                        ks.getValue()
                                                                .stream()
                                                                .map(UserRole::getRole)
                                                                .collect(Collectors.toList())
                                                )
                                        ).collect(Collectors.toList())
                                , PASSWORD_HOLDER)
                ).collect(Collectors.toList());
    }
}
